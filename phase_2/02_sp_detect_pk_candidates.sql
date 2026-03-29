-- ============================================================================
-- NEXUS DV2.0 — Phase 2: SP_DETECT_PK_CANDIDATES
-- Scores and ranks PK candidates from profiling results for a given run.
-- Handles single-column and 2-column composite candidates.
-- Writes results to META.DV_PK_CANDIDATES.
--
-- Scoring per spec §7:
--   Uniqueness = 1.0              → +40
--   Uniqueness > 0.95             → +25
--   Null % = 0                    → +20
--   Name contains ID/KEY/NBR/NUM/CODE → +15
--   Numeric type or short VARCHAR  → +10
--   First ordinal position        → +5
--   Composite 2-col uniqueness=1.0 → +30 (base score)
--   Composite 3-col uniqueness=1.0 → +20 (base score)
--
-- Call: CALL META.SP_DETECT_PK_CANDIDATES('your-run-id');
-- Returns: number of candidates detected
-- ============================================================================

USE SCHEMA NEXUS.META;

CREATE OR REPLACE PROCEDURE META.SP_DETECT_PK_CANDIDATES(RUN_ID VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
COMMENT = 'Scores and inserts PK candidates for a profiling run into META.DV_PK_CANDIDATES.'
AS $$
import json

def run(session, run_id: str) -> str:

    def esc(v):
        if v is None:
            return 'NULL'
        return "'" + str(v).replace("'", "''") + "'"

    s_run_id = run_id.replace("'", "''")

    # ── Get run context ───────────────────────────────────────────────────────

    run_info = session.sql(f"""
        SELECT SOURCE_SCHEMA, SOURCE_TABLE, ROW_COUNT
        FROM META.DV_PROFILING_RUN
        WHERE RUN_ID = '{s_run_id}' AND STATUS = 'COMPLETED'
    """).collect()

    if not run_info:
        raise ValueError(f"No completed profiling run found for RUN_ID = {run_id}")

    source_schema = run_info[0]['SOURCE_SCHEMA']
    source_table  = run_info[0]['SOURCE_TABLE']
    row_count     = run_info[0]['ROW_COUNT'] or 0

    s_schema = source_schema.replace("'", "''")
    s_table  = source_table.replace("'",  "''")

    # ── Clear previous candidates for this run ────────────────────────────────

    session.sql(f"DELETE FROM META.DV_PK_CANDIDATES WHERE RUN_ID = '{s_run_id}'").collect()

    # ── Single-column scoring ─────────────────────────────────────────────────

    cols = session.sql(f"""
        SELECT
            COLUMN_NAME,
            SOURCE_DATA_TYPE,
            INFERRED_DATA_TYPE,
            UNIQUENESS_RATIO,
            NULL_PERCENTAGE,
            MAX_LENGTH,
            ORDINAL_POSITION
        FROM META.DV_PROFILING_RESULTS
        WHERE RUN_ID = '{s_run_id}'
        ORDER BY ORDINAL_POSITION
    """).collect()

    single_candidates = []

    for c in cols:
        col_name    = c['COLUMN_NAME']
        uniq        = float(c['UNIQUENESS_RATIO'] or 0)
        null_pct    = float(c['NULL_PERCENTAGE']  or 0)
        inferred_dt = (c['INFERRED_DATA_TYPE'] or '').upper()
        source_dt   = (c['SOURCE_DATA_TYPE']   or '').upper()
        max_len     = c['MAX_LENGTH'] or 999
        ordinal     = c['ORDINAL_POSITION'] or 99
        col_upper   = col_name.upper()

        # Score components
        s_uniqueness  = 40 if uniq == 1.0 else (25 if uniq >= 0.95 else 0)
        s_not_null    = 20 if null_pct == 0 else 0
        s_name        = 15 if any(x in col_upper for x in ['_ID','_KEY','_NBR','_NUM','_CODE','ID','KEY','NBR','NUM','CODE']) else 0
        numeric_types = {'NUMBER','INT','INTEGER','BIGINT','SMALLINT','DECIMAL','NUMERIC','BYTEINT','TINYINT','FLOAT','DOUBLE'}
        s_type        = 10 if (inferred_dt == 'NUMBER' or source_dt in numeric_types or
                               (inferred_dt == 'VARCHAR' and max_len <= 20)) else 0
        s_position    = 5 if ordinal == 1 else 0

        total_score = s_uniqueness + s_not_null + s_name + s_type + s_position

        breakdown = json.dumps({
            'uniqueness': s_uniqueness,
            'not_null':   s_not_null,
            'name':       s_name,
            'data_type':  s_type,
            'position':   s_position
        })

        # Only include candidates with a meaningful score
        if total_score >= 40:
            session.sql(f"""
                INSERT INTO META.DV_PK_CANDIDATES
                    (RUN_ID, SOURCE_TABLE, COLUMN_NAMES, CANDIDATE_TYPE, PK_SCORE, SCORE_BREAKDOWN)
                SELECT
                    {esc(run_id)},
                    {esc(source_table)},
                    ARRAY_CONSTRUCT({esc(col_name)}),
                    'SINGLE',
                    {total_score},
                    PARSE_JSON({esc(breakdown)})
            """).collect()
            single_candidates.append({'col': col_name, 'score': total_score, 'uniq': uniq, 'null_pct': null_pct})

    # ── Composite 2-column candidates ─────────────────────────────────────────
    # Only attempt if no strong single-column candidate (score >= 60) was found
    # or if the best single-column candidate is under 60.

    strong_single = any(c['score'] >= 60 for c in single_candidates)

    if not strong_single and row_count > 0 and len(cols) >= 2:
        # Collect possible columns (score 25-59, or high uniqueness individually)
        composite_candidates_cols = [
            c['COLUMN_NAME'] for c in cols
            if float(c['UNIQUENESS_RATIO'] or 0) >= 0.5 and float(c['NULL_PERCENTAGE'] or 0) == 0
        ][:6]  # Limit to first 6 to avoid explosion of combinations

        q_schema = '"' + source_schema.replace('"', '""') + '"'
        q_table  = '"' + source_table.replace('"',  '""') + '"'
        full_tbl = f'{q_schema}.{q_table}'

        # Try each 2-column combination
        tested_combos = set()
        for i, col_a in enumerate(composite_candidates_cols):
            for col_b in composite_candidates_cols[i+1:]:
                combo_key = tuple(sorted([col_a, col_b]))
                if combo_key in tested_combos:
                    continue
                tested_combos.add(combo_key)

                qa = '"' + col_a.replace('"', '""') + '"'
                qb = '"' + col_b.replace('"', '""') + '"'

                combo_distinct = session.sql(f"""
                    SELECT COUNT(*) AS COMBO_DISTINCT
                    FROM (SELECT DISTINCT {qa}, {qb} FROM {full_tbl}) t
                """).collect()[0][0]

                combo_uniq = float(combo_distinct) / float(row_count) if row_count > 0 else 0.0

                if combo_uniq >= 0.95:
                    score    = 30 if combo_uniq == 1.0 else 20
                    breakdown = json.dumps({
                        'type':    'COMPOSITE_2COL',
                        'columns': [col_a, col_b],
                        'uniqueness_ratio': round(combo_uniq, 6)
                    })
                    col_names_sorted = sorted([col_a, col_b])  # alphabetical per DV2.0 convention
                    session.sql(f"""
                        INSERT INTO META.DV_PK_CANDIDATES
                            (RUN_ID, SOURCE_TABLE, COLUMN_NAMES, CANDIDATE_TYPE, PK_SCORE, SCORE_BREAKDOWN)
                        SELECT
                            {esc(run_id)},
                            {esc(source_table)},
                            ARRAY_CONSTRUCT({esc(col_names_sorted[0])}, {esc(col_names_sorted[1])}),
                            'COMPOSITE',
                            {score},
                            PARSE_JSON({esc(breakdown)})
                    """).collect()

    # ── Return summary ─────────────────────────────────────────────────────────

    total = session.sql(f"""
        SELECT COUNT(*) AS C FROM META.DV_PK_CANDIDATES WHERE RUN_ID = '{s_run_id}'
    """).collect()[0][0]

    return f"Detected {total} PK candidate(s) for {source_table} (run: {run_id})"

$$;

-- ── Grant ─────────────────────────────────────────────────────────────────────
-- GRANT USAGE ON PROCEDURE META.SP_DETECT_PK_CANDIDATES(VARCHAR) TO ROLE NEXUS_MODELER;

-- ── Quick test ────────────────────────────────────────────────────────────────
-- CALL META.SP_DETECT_PK_CANDIDATES('your-run-id-from-sp-profile-table');

-- Review results:
-- SELECT COLUMN_NAMES, CANDIDATE_TYPE, PK_SCORE, SCORE_BREAKDOWN
-- FROM META.DV_PK_CANDIDATES
-- WHERE RUN_ID = 'your-run-id'
-- ORDER BY PK_SCORE DESC;
