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

        # For very wide tables (>60 cols) suppress top_values and pattern
        # to keep the prompt within token budget.
        wide_table = len(prof_rows) > 60
        if wide_table:
            profiling_lines.append(
                f"  NOTE: Wide table ({len(prof_rows)} columns). "
                "Top-values and pattern detail omitted to save space. "
                "Focus on column names and data types for classification.")

        for r in prof_rows:
            pk_flag = " *** PK CANDIDATE ***" if r['IS_PK_CANDIDATE'] else ""
            if wide_table:
                # Compact format for wide tables
                line = (f"  {r['ORDINAL_POSITION']}. {r['COLUMN_NAME']}"
                        f" | {r['SOURCE_DATA_TYPE']}"
                        f" | uniq:{r['UNIQ_PCT']}% null:{r['NULL_PCT']}%"
                        f" | chg:{r['CHANGE_FREQUENCY'] or '?'}"
                        f"{pk_flag}")
            else:
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

    # ── 6. Query approved column definitions from internal store ─────────────
    # These persist across sessions and work for datashares/read-only sources.
    approved_col_defs_section = ""
    try:
        src_sch = source_schema or (
            session.sql(f"""
                SELECT SOURCE_SCHEMA FROM META.DV_PROFILING_RUN
                WHERE RUN_ID = '{safe(run_id or "")}'
                LIMIT 1
            """).collect()[0]['SOURCE_SCHEMA'] if run_id else None
        )
        if src_sch:
            def_rows = session.sql(f"""
                SELECT COLUMN_NAME, DEFINITION, IS_SENSITIVE, TABLE_DESCRIPTION
                FROM META.DV_COLUMN_DEFINITIONS
                WHERE SOURCE_SCHEMA = '{safe(src_sch)}'
                  AND SOURCE_TABLE  = '{safe(source_table)}'
                ORDER BY COLUMN_NAME
            """).collect()
        else:
            def_rows = session.sql(f"""
                SELECT COLUMN_NAME, DEFINITION, IS_SENSITIVE, TABLE_DESCRIPTION
                FROM META.DV_COLUMN_DEFINITIONS
                WHERE SOURCE_TABLE = '{safe(source_table)}'
                ORDER BY COLUMN_NAME
            """).collect()

        if def_rows:
            def_lines   = [f"  {r['COLUMN_NAME']}: {r['DEFINITION']}"
                           for r in def_rows if r['DEFINITION']]
            sens_lines  = [f"  {r['COLUMN_NAME']}: {r['IS_SENSITIVE']}"
                           for r in def_rows
                           if r['IS_SENSITIVE'] and r['IS_SENSITIVE'] not in ('None', None)]
            tbl_desc    = next((r['TABLE_DESCRIPTION'] for r in def_rows
                                if r['TABLE_DESCRIPTION']), None)
            parts = []
            if tbl_desc:
                parts.append(f"TABLE DESCRIPTION: {tbl_desc}")
            if def_lines:
                parts.append(
                    "APPROVED COLUMN DEFINITIONS — USE THESE VERBATIM as column_definition "
                    "values in the vault JSON. Do not rephrase or shorten.\n"
                    + "\n".join(def_lines))
            if sens_lines:
                parts.append("SENSITIVE COLUMNS:\n" + "\n".join(sens_lines))
            if parts:
                approved_col_defs_section = (
                    "\n=== APPROVED COLUMN DEFINITIONS FROM NEXUS REGISTRY ===\n"
                    + "\n\n".join(parts)
                    + "\n=== END APPROVED COLUMN DEFINITIONS ===")
    except Exception:
        pass

    # ── 7. Build user message ─────────────────────────────────────────────────
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
{approved_col_defs_section}

{registry_context}

{abbr_context}

{data_context}

INSTRUCTIONS:
1. Read the MODELER NOTES above before doing anything else. Apply every instruction in them.
2. Check the EXISTING APPROVED REGISTRY for hub reuse opportunities before creating any new hub.
3. Apply the satellite simplification guardrails: consolidate where possible, avoid over-splitting.
4. Apply entity naming conventions: SAT_<SRCCODE>_<NOUN>_<DESC> — source system code comes FIRST after SAT_, single underscore throughout. Max 5-char source code. Same pattern for MSAT and ESAT. Examples: SAT_ACCTS_CUSTOMER_DETAILS, MSAT_ACCTS_CUSTOMER_PHONE, ESAT_ACCTS_ACCOUNT_CUSTOMER.
5. CRITICAL — SOURCE COLUMN NAMES: For every attribute (column_role: ATTR) and business key (column_role: BK) column,
   the "column_name" field MUST be identical to the source column name as it appears in the profiling data.
   Do NOT abbreviate, rename, translate, or transform attribute or business key column names.
   Only vault-generated metadata columns (HK, HASHDIFF, LOAD_DTS, REC_SRC) use vault naming conventions.
   The "source_column" field must also be set to the exact source column name.
6. Include BATCH_ID in every entity's column list.
7. In each entity rationale, state explicitly whether modeler notes, statistics, or name heuristics drove the decision.
8. APPROVED COLUMN DEFINITIONS: If the section "APPROVED COLUMN DEFINITIONS FROM NEXUS REGISTRY" is present above,
   copy those definitions verbatim into the column_definition field of the matching ATTR or BK columns.
   Do NOT paraphrase, shorten, or rewrite them. These are modeler-reviewed and approved.
9. Keep column_definition values SHORT (10 words max) for columns without an approved definition.
   For wide tables with many columns, omit column_definition entirely rather than writing long descriptions.
10. Return ONLY the JSON object — no markdown fences, no explanation outside the JSON."""

    # ── 7. Call Cortex AI_COMPLETE ─────────────────────────────────────────────
    # max_tokens inlined as a literal (not a bind param) — Snowflake requires
    # the options object to be a literal or OBJECT_CONSTRUCT, not PARSE_JSON(?).
    messages_json = json.dumps([
        {"role": "system", "content": system_prompt},
        {"role": "user",   "content": user_message}
    ], ensure_ascii=True)

    ai_row = session.sql(
        """SELECT SNOWFLAKE.CORTEX.AI_COMPLETE(
               ?,
               PARSE_JSON(?),
               OBJECT_CONSTRUCT('max_tokens', 20000, 'temperature', 0)
           )::VARCHAR AS AI_RESPONSE""",
        params=["claude-opus-4-6", messages_json]
    ).collect()

    # SNOWFLAKE.CORTEX.AI_COMPLETE(...)::VARCHAR returns the content string directly
    # (not a JSON envelope). Strip markdown fences if present, then parse.
    ai_text = (ai_row[0]['AI_RESPONSE'] or '').strip() if ai_row else ''

    # ── 8. Parse AI response ──────────────────────────────────────────────────
    def _extract_json_object(text):
        """
        Extract the outermost {...} from text using balanced-brace matching,
        skipping over strings so braces inside string values are ignored.
        More reliable than rfind('}') when text has trailing content.
        """
        start = text.find('{')
        if start == -1:
            return text
        depth = 0
        in_str = False
        escaped = False
        for i in range(start, len(text)):
            ch = text[i]
            if escaped:
                escaped = False
                continue
            if ch == '\\' and in_str:
                escaped = True
                continue
            if ch == '"':
                in_str = not in_str
                continue
            if in_str:
                continue
            if ch == '{':
                depth += 1
            elif ch == '}':
                depth -= 1
                if depth == 0:
                    return text[start:i + 1]
        # Fell off the end — return everything from start (malformed but try anyway)
        return text[start:]

    def _repair_truncated(text):
        """
        If the JSON was cut off mid-stream (unterminated string / unexpected EOF),
        walk back from the end to find the last position that completed a full
        value at depth >= 1, then close all open brackets/braces.

        Handles two truncation cases:
          1. Truncated mid-string — close the open string with '"', strip
             the partial key/value, then close open scopes.
          2. Truncated at a bracket boundary — trim to last safe position
             and close open scopes.
        """
        stack     = []   # '{' or '[' for each open scope
        in_str    = False
        escaped   = False
        last_safe = 0    # byte offset after the last cleanly closed value
        str_start = -1   # where the current open string started

        for i, ch in enumerate(text):
            if escaped:
                escaped = False
                continue
            if ch == '\\' and in_str:
                escaped = True
                continue
            if ch == '"':
                if in_str:
                    in_str    = False
                    str_start = -1
                    # A closing quote at depth >= 1 is a safe point only if
                    # the next non-space char is : , } ] (i.e. complete value)
                    # — we record it conservatively below via the comma/bracket logic
                else:
                    in_str    = True
                    str_start = i
                continue
            if in_str:
                continue
            if ch in ('{', '['):
                stack.append(ch)
            elif ch in ('}', ']'):
                if stack:
                    stack.pop()
                if stack:
                    last_safe = i + 1
            elif ch == ',' and stack:
                last_safe = i + 1   # just after comma = clean boundary

        if not stack and not in_str:
            return text   # already balanced

        if in_str:
            # Truncated mid-string: roll back to the last safe bracket boundary
            # (dropping the partial string and its key if present)
            text = text[:last_safe].rstrip().rstrip(',')
            # Re-derive the remaining open stack from the trimmed text
            stack2, in_s2, esc2 = [], False, False
            for ch in text:
                if esc2:            esc2 = False; continue
                if ch == '\\' and in_s2: esc2 = True; continue
                if ch == '"':       in_s2 = not in_s2; continue
                if in_s2:           continue
                if ch in ('{','['): stack2.append(ch)
                elif ch in ('}',']') and stack2: stack2.pop()
            stack = stack2
        else:
            # Truncated at a bracket boundary — trim to last clean position
            text = text[:last_safe].rstrip().rstrip(',')

        for opener in reversed(stack):
            text += ']' if opener == '[' else '}'
        return text

    def _parse_ai_json(text):
        """Robustly parse AI-generated JSON with layered fallbacks."""
        # Strip markdown fences
        text = re.sub(r'```(?:json)?\s*', '', text)
        text = re.sub(r'```',             '', text)
        text = text.strip()

        # Extract outermost {...} via balanced-brace walk
        text = _extract_json_object(text)

        last_err = "unknown parse error"

        # Pass 1: standard parse
        try:
            return json.loads(text), None
        except Exception as e1:
            last_err = str(e1)

        # Pass 2: fix trailing commas
        t2 = re.sub(r',(\s*[}\]])', r'\1', text)
        try:
            return json.loads(t2), None
        except Exception as e2:
            last_err = str(e2)

        # Pass 3: Python literals → JSON booleans/null
        t3 = re.sub(r'\bTrue\b',  'true',  t2)
        t3 = re.sub(r'\bFalse\b', 'false', t3)
        t3 = re.sub(r'\bNone\b',  'null',  t3)
        try:
            return json.loads(t3), None
        except Exception as e3:
            last_err = str(e3)

        # Pass 4: truncation recovery — close any open braces/brackets then retry
        t4 = _repair_truncated(t3)
        t4 = re.sub(r',(\s*[}\]])', r'\1', t4)   # trailing commas on repaired text
        try:
            result = json.loads(t4)
            # Mark that we recovered from truncation so a warning is attached
            if isinstance(result, dict):
                result.setdefault('warnings', []).append(
                    'AI response was truncated and partially recovered. '
                    'Some columns or entities near the end may be missing. '
                    'Consider re-generating.')
            return result, None
        except Exception as e4:
            last_err = str(e4)

        return None, last_err

    if ai_text:
        parsed, parse_err = _parse_ai_json(ai_text)
        if parsed is not None:
            proposal_data = parsed
        else:
            # Store first 8000 chars of raw response for UI diagnosis
            proposal_data = {
                "confidence_overall": "INFERRED",
                "input_scenario":     input_scenario,
                "warnings":           [f"AI response could not be parsed as JSON: {(parse_err or '')[:500]}",
                                       f"Raw response length: {len(ai_text)} chars. Manual review required."],
                "hubs": [], "links": [], "satellites": [],
                "hash_definitions": [],
                "_raw_ai_response": ai_text[:8000]
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
