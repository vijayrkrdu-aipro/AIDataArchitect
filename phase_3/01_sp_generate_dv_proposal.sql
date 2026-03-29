-- ============================================================================
-- NEXUS DV2.0 — Phase 3: SP_GENERATE_DV_PROPOSAL
-- Calls Cortex AI_COMPLETE (claude-opus-4-6) with the DV2.0 system prompt,
-- registry context, and profiling/metadata for a source table.
-- Writes to META.DV_DESIGN_PROPOSAL and creates a DRAFT DV_DESIGN_WORKSPACE.
--
-- Parameters:
--   SOURCE_TABLE     — source table name
--   SOURCE_SYSTEM    — source system code (e.g. ACCT_SYS)
--   SOURCE_SCHEMA    — source schema (optional)
--   SOURCE_DATABASE  — source database (optional)
--   RUN_ID           — specific profiling run to use (optional — auto-detects latest)
--
-- Returns: workspace_id (VARCHAR) — the DRAFT workspace created for editing
-- ============================================================================

USE SCHEMA NEXUS.META;

CREATE OR REPLACE PROCEDURE META.SP_GENERATE_DV_PROPOSAL(
    SOURCE_TABLE     VARCHAR,
    SOURCE_SYSTEM    VARCHAR,
    SOURCE_SCHEMA    VARCHAR DEFAULT NULL,
    SOURCE_DATABASE  VARCHAR DEFAULT NULL,
    RUN_ID           VARCHAR DEFAULT NULL,
    MODELER_NOTES    VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
COMMENT = 'Generates an AI-assisted DV2.0 design proposal using Cortex claude-opus-4-6. Returns workspace_id.'
AS $$
import uuid
import json
import re

def run(session,
        source_table:    str,
        source_system:   str,
        source_schema:   str = None,
        source_database: str = None,
        run_id:          str = None,
        modeler_notes:   str = None) -> str:

    # ── IDs ───────────────────────────────────────────────────────────────────
    proposal_id  = str(uuid.uuid4())
    workspace_id = str(uuid.uuid4())

    def esc(v):
        if v is None:
            return 'NULL'
        return "'" + str(v).replace("'", "''") + "'"

    def safe(v):
        return str(v).replace("'", "''") if v is not None else ''

    # ── 1. Assemble system prompt ─────────────────────────────────────────────
    sp_row = session.sql("""
        SELECT LISTAGG(SECTION_CONTENT, '\n\n')
               WITHIN GROUP (ORDER BY SECTION_ORDER) AS PROMPT
        FROM META.DV_AI_SYSTEM_PROMPT
        WHERE IS_ACTIVE = TRUE AND VERSION = '1.0'
    """).collect()
    system_prompt = sp_row[0]['PROMPT'] if sp_row and sp_row[0]['PROMPT'] else ''
    prompt_version = '1.0'

    # ── 2. Abbreviation table context ─────────────────────────────────────────
    abbr_rows = session.sql("""
        SELECT PHYSICAL_ABBR, LOGICAL_NAME, DOMAIN
        FROM META.DV_ABBREVIATION
        WHERE IS_ACTIVE = TRUE
        ORDER BY PHYSICAL_ABBR
    """).collect()
    abbr_lines = [f"  {r['PHYSICAL_ABBR']} = {r['LOGICAL_NAME']} (domain: {r['DOMAIN'] or 'n/a'})"
                  for r in abbr_rows]
    abbr_context = "ABBREVIATION TABLE:\n" + "\n".join(abbr_lines) if abbr_lines else "ABBREVIATION TABLE: (empty)"

    # ── 3. Registry context (approved entities only) ──────────────────────────
    reg_rows = session.sql("""
        SELECT
            e.ENTITY_ID, e.ENTITY_TYPE, e.LOGICAL_NAME,
            e.SOURCE_SYSTEM, e.PARENT_ENTITY_ID, e.DOMAIN,
            LISTAGG(c.COLUMN_NAME || ':' || c.COLUMN_ROLE || ':' || c.DATA_TYPE, ', ')
                WITHIN GROUP (ORDER BY c.ORDINAL_POSITION) AS COLS
        FROM META.DV_ENTITY e
        LEFT JOIN META.DV_ENTITY_COLUMN c ON c.ENTITY_ID = e.ENTITY_ID
        WHERE e.APPROVAL_STATUS = 'APPROVED' AND e.IS_ACTIVE = TRUE
        GROUP BY e.ENTITY_ID, e.ENTITY_TYPE, e.LOGICAL_NAME,
                 e.SOURCE_SYSTEM, e.PARENT_ENTITY_ID, e.DOMAIN
        ORDER BY e.ENTITY_TYPE, e.ENTITY_ID
    """).collect()

    if reg_rows:
        reg_lines = []
        for r in reg_rows:
            line = (f"  {r['ENTITY_ID']} [{r['ENTITY_TYPE']}]"
                    f" — {r['LOGICAL_NAME'] or 'no name'}")
            if r['PARENT_ENTITY_ID']:
                line += f" (parent: {r['PARENT_ENTITY_ID']})"
            if r['COLS']:
                line += f"\n    Columns: {r['COLS']}"
            reg_lines.append(line)
        registry_context = "EXISTING APPROVED REGISTRY:\n" + "\n".join(reg_lines)
    else:
        registry_context = "EXISTING APPROVED REGISTRY: (empty — this is the first model)"

    # ── 4. Find profiling run ─────────────────────────────────────────────────
    if not run_id or not run_id.strip():
        latest = session.sql(f"""
            SELECT RUN_ID FROM META.DV_PROFILING_RUN
            WHERE SOURCE_TABLE  = '{safe(source_table)}'
              AND SOURCE_SYSTEM = '{safe(source_system)}'
              AND STATUS = 'COMPLETED'
            ORDER BY COMPLETED_AT DESC
            LIMIT 1
        """).collect()
        run_id = latest[0]['RUN_ID'] if latest else None

    # ── 5. Build profiling / metadata context ─────────────────────────────────
    if run_id:
        input_scenario = 'FULL_PROFILING'

        run_meta = session.sql(f"""
            SELECT SOURCE_DATABASE, SOURCE_SCHEMA, ROW_COUNT, COLUMN_COUNT, PROFILING_METHOD
            FROM META.DV_PROFILING_RUN WHERE RUN_ID = '{safe(run_id)}'
        """).collect()
        rm = run_meta[0].as_dict() if run_meta else {}

        prof_rows = session.sql(f"""
            SELECT
                COLUMN_NAME, ORDINAL_POSITION, SOURCE_DATA_TYPE, INFERRED_DATA_TYPE,
                ROUND(UNIQUENESS_RATIO*100,2)  AS UNIQ_PCT,
                ROUND(NULL_PERCENTAGE,2)        AS NULL_PCT,
                DISTINCT_COUNT, MIN_VALUE, MAX_VALUE,
                TOP_VALUES::VARCHAR             AS TOP_VALS,
                PATTERN_DETECTED, CHANGE_FREQUENCY,
                IS_PK_CANDIDATE
            FROM META.DV_PROFILING_RESULTS
            WHERE RUN_ID = '{safe(run_id)}'
            ORDER BY ORDINAL_POSITION
        """).collect()

        pk_rows = session.sql(f"""
            SELECT COLUMN_NAMES::VARCHAR AS COLS, CANDIDATE_TYPE, PK_SCORE, MODELER_SELECTED
            FROM META.DV_PK_CANDIDATES
            WHERE RUN_ID = '{safe(run_id)}'
            ORDER BY MODELER_SELECTED DESC, PK_SCORE DESC
            LIMIT 5
        """).collect()

        profiling_lines = [
            f"SOURCE TABLE: {source_database or rm.get('SOURCE_DATABASE','?')}"
            f".{source_schema or rm.get('SOURCE_SCHEMA','?')}.{source_table}",
            f"SOURCE SYSTEM: {source_system}",
            f"ROW COUNT: {rm.get('ROW_COUNT','?'):,}" if rm.get('ROW_COUNT') else f"ROW COUNT: ?",
            f"COLUMN COUNT: {rm.get('COLUMN_COUNT','?')}",
            f"PROFILING METHOD: {rm.get('PROFILING_METHOD','?')}",
            "",
            "COLUMN PROFILING RESULTS:",
        ]
        for r in prof_rows:
            pk_flag = " *** PK CANDIDATE ***" if r['IS_PK_CANDIDATE'] else ""
            line = (f"  {r['ORDINAL_POSITION']}. {r['COLUMN_NAME']}"
                    f" | src:{r['SOURCE_DATA_TYPE']} inferred:{r['INFERRED_DATA_TYPE']}"
                    f" | uniq:{r['UNIQ_PCT']}% null:{r['NULL_PCT']}%"
                    f" | chg:{r['CHANGE_FREQUENCY'] or '?'}"
                    f" | pattern:{r['PATTERN_DETECTED'] or 'none'}"
                    f" | top:{r['TOP_VALS'] or '[]'}"
                    f"{pk_flag}")
            profiling_lines.append(line)

        if pk_rows:
            profiling_lines.append("")
            profiling_lines.append("PK CANDIDATE SCORES:")
            for pk in pk_rows:
                sel = " ← MODELER CONFIRMED" if pk['MODELER_SELECTED'] else ""
                profiling_lines.append(
                    f"  {pk['COLS']} [{pk['CANDIDATE_TYPE']}] score={pk['PK_SCORE']}{sel}")

        data_context = "\n".join(profiling_lines)

    else:
        # No profiling — try to get column names from INFORMATION_SCHEMA
        input_scenario = 'COLUMN_NAMES_ONLY'
        try:
            q_db  = f'"{source_database}"' if source_database else 'CURRENT_DATABASE()'
            cols  = session.sql(f"""
                SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION
                FROM {q_db}.INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = UPPER('{safe(source_schema or "")}')
                  AND TABLE_NAME   = UPPER('{safe(source_table)}')
                ORDER BY ORDINAL_POSITION
            """).collect()
            if cols:
                input_scenario = 'COLUMN_NAMES_ONLY'
                col_lines = [f"  {c['ORDINAL_POSITION']}. {c['COLUMN_NAME']} ({c['DATA_TYPE']})"
                             for c in cols]
                data_context = (
                    f"SOURCE TABLE: {source_table}\nSOURCE SYSTEM: {source_system}\n"
                    "NOTE: No profiling data available. Column names only.\n\n"
                    "COLUMNS:\n" + "\n".join(col_lines)
                )
            else:
                input_scenario = 'DATA_INFERENCE'
                data_context = (
                    f"SOURCE TABLE: {source_table}\nSOURCE SYSTEM: {source_system}\n"
                    "NOTE: No profiling or column metadata available. Infer from table name only."
                )
        except Exception:
            input_scenario = 'DATA_INFERENCE'
            data_context = (
                f"SOURCE TABLE: {source_table}\nSOURCE SYSTEM: {source_system}\n"
                "NOTE: No profiling or column metadata available. Infer from table name only."
            )

    # ── 6. Build user message ─────────────────────────────────────────────────
    if modeler_notes and modeler_notes.strip():
        notes_text = modeler_notes.strip()
        modeler_notes_section = f"""=== MODELER NOTES — READ THIS FIRST AND APPLY TO EVERY DECISION ===
{notes_text}

Instructions for using these notes:
- Any primary key stated here OVERRIDES all statistical PK detection. Use it exactly as written.
- Any column definition stated here OVERRIDES inference from column names or statistics.
- Any column marked as deprecated or to-be-ignored must be EXCLUDED from all vault entities.
- Any relationship to another table mentioned here means you MUST propose a link to that entity.
- Any statement about table purpose directly guides which hub(s) to create.
- In every entity rationale, explicitly state which part of the modeler notes influenced that decision.
=== END MODELER NOTES ==="""
    else:
        modeler_notes_section = "MODELER NOTES: None provided. Rely on profiling data and column name heuristics."

    user_message = f"""Generate a Data Vault 2.0 Raw Vault design proposal for the source table below.

{modeler_notes_section}

{registry_context}

{abbr_context}

{data_context}

INSTRUCTIONS:
1. Read the MODELER NOTES above before doing anything else. Apply every instruction in them.
2. Check the EXISTING APPROVED REGISTRY for hub reuse opportunities before creating any new hub.
3. Apply the satellite simplification guardrails: consolidate where possible, avoid over-splitting.
4. Apply all naming conventions: SAT_<NOUN>_<DESC>__<SRC> with max 5-char source code.
5. Include BATCH_ID in every entity's column list.
6. In each entity rationale, state explicitly whether modeler notes, statistics, or name heuristics drove the decision.
7. Return ONLY the JSON object — no markdown fences, no explanation outside the JSON."""

    # ── 7. Call Cortex AI_COMPLETE ─────────────────────────────────────────────
    # Use parameterized binding — avoids all dollar-quoting and size issues.
    messages_json = json.dumps([
        {"role": "system", "content": system_prompt},
        {"role": "user",   "content": user_message}
    ], ensure_ascii=True)

    ai_row = session.sql(
        "SELECT SNOWFLAKE.CORTEX.AI_COMPLETE(?, PARSE_JSON(?))::VARCHAR AS AI_RESPONSE",
        params=["claude-opus-4-6", messages_json]
    ).collect()

    # SNOWFLAKE.CORTEX.AI_COMPLETE(...)::VARCHAR returns the content string directly
    # (not a JSON envelope). Strip markdown fences if present, then parse.
    ai_text = (ai_row[0]['AI_RESPONSE'] or '').strip() if ai_row else ''

    # ── 8. Parse AI response ──────────────────────────────────────────────────
    if ai_text:
        # Strip markdown code fences if AI wrapped in them
        if ai_text.startswith('```'):
            ai_text = re.sub(r'^```(?:json)?\s*', '', ai_text, flags=re.MULTILINE)
            ai_text = re.sub(r'\s*```\s*$',        '', ai_text, flags=re.MULTILINE)
        ai_text = ai_text.strip()
        try:
            proposal_data = json.loads(ai_text)
        except Exception as parse_err:
            proposal_data = {
                "confidence_overall": "INFERRED",
                "input_scenario":     input_scenario,
                "warnings":           [f"AI response could not be parsed as JSON: {str(parse_err)[:200]}",
                                       "Raw response stored. Manual review required."],
                "hubs": [], "links": [], "satellites": [],
                "hash_definitions": [],
                "_raw_ai_response": ai_text[:2000]
            }
    else:
        # AI returned empty response
        proposal_data = {
            "confidence_overall": "INFERRED",
            "input_scenario":     input_scenario,
            "warnings":           ["AI_COMPLETE returned an empty response. Check model availability."],
            "hubs": [], "links": [], "satellites": [],
            "hash_definitions": []
        }

    confidence_overall = proposal_data.get('confidence_overall', 'MEDIUM')

    # ── 9. Store proposal ─────────────────────────────────────────────────────
    proposal_json = json.dumps(proposal_data, ensure_ascii=True)

    session.sql(
        """INSERT INTO META.DV_DESIGN_PROPOSAL
               (PROPOSAL_ID, SOURCE_SYSTEM, SOURCE_SCHEMA, SOURCE_TABLE,
                RUN_ID, INPUT_SCENARIO, AI_MODEL, PROMPT_VERSION,
                PROPOSAL_JSON, CONFIDENCE, STATUS)
           SELECT ?, ?, ?, ?, ?, ?, 'claude-opus-4-6', ?, PARSE_JSON(?), ?, 'PENDING'""",
        params=[proposal_id, source_system, source_schema, source_table,
                run_id, input_scenario, prompt_version,
                proposal_json, confidence_overall]
    ).collect()

    # ── 10. Create DRAFT workspace ────────────────────────────────────────────
    workspace_data = dict(proposal_data)
    workspace_data['_meta'] = {
        'proposal_id':    proposal_id,
        'source_table':   source_table,
        'source_system':  source_system,
        'source_schema':  source_schema,
        'modeler_notes':  modeler_notes or '',
        'run_id':         run_id,
        'input_scenario': input_scenario
    }
    for entity_list in ('hubs', 'links', 'satellites'):
        for entity in workspace_data.get(entity_list, []):
            if 'entity_status' not in entity:
                entity['entity_status'] = 'EXISTING' if not entity.get('is_new', True) else 'NEW'

    workspace_json = json.dumps(workspace_data, ensure_ascii=True)

    session.sql(
        """INSERT INTO META.DV_DESIGN_WORKSPACE
               (WORKSPACE_ID, PROPOSAL_ID, SOURCE_TABLE, SOURCE_SYSTEM, SOURCE_SCHEMA,
                WORKSPACE_JSON, STATUS, VERSION_NUMBER, AI_CONFIDENCE, INPUT_SCENARIO)
           SELECT ?, ?, ?, ?, ?, PARSE_JSON(?), 'DRAFT', 1, ?, ?""",
        params=[workspace_id, proposal_id, source_table, source_system, source_schema,
                workspace_json, confidence_overall, input_scenario]
    ).collect()

    # ── 11. Audit log ─────────────────────────────────────────────────────────
    audit_payload = json.dumps({
        "proposal_id":    proposal_id,
        "workspace_id":   workspace_id,
        "input_scenario": input_scenario,
        "confidence":     confidence_overall,
        "hubs":           len(proposal_data.get('hubs', [])),
        "links":          len(proposal_data.get('links', [])),
        "satellites":     len(proposal_data.get('satellites', [])),
        "run_id":         run_id
    })
    session.sql(
        """INSERT INTO META.DV_AUDIT_LOG
               (ACTION_TYPE, ENTITY_TYPE, ENTITY_ID, SOURCE_TABLE, SOURCE_SYSTEM, ACTION_DETAILS)
           SELECT 'GENERATE', 'WORKSPACE', ?, ?, ?, PARSE_JSON(?)""",
        params=[workspace_id, source_table, source_system, audit_payload]
    ).collect()

    return workspace_id

$$;

-- ── Grant ─────────────────────────────────────────────────────────────────────
-- GRANT USAGE ON PROCEDURE META.SP_GENERATE_DV_PROPOSAL(VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR)
--   TO ROLE NEXUS_MODELER;

-- ── Test ──────────────────────────────────────────────────────────────────────
-- CALL META.SP_GENERATE_DV_PROPOSAL('ACCT_MSTR', 'ACCT_SYS', 'STAGING', 'NEXUS', NULL);
