-- ============================================================================
-- NEXUS DV2.0 — Phase 2: SP_DETECT_CHANGE_FREQUENCY
-- Updates CHANGE_FREQUENCY in META.DV_PROFILING_RESULTS for a given run.
-- Works against any database — reads SOURCE_DATABASE from the profiling run.
--
-- Strategy (priority order):
--   1. Snapshot comparison — looks for companion _HIST/_SNAP table in the
--      same database.schema, joins on the confirmed/best PK from
--      META.DV_PK_CANDIDATES, compares column values across two snapshots.
--   2. Semantic classification — column name pattern matching (fallback).
--      Covers generic DV patterns + demographics domain vocabulary.
--
-- Call: CALL META.SP_DETECT_CHANGE_FREQUENCY('your-run-id');
-- Returns: summary of classifications applied
-- ============================================================================

USE SCHEMA NEXUS.META;

CREATE OR REPLACE PROCEDURE META.SP_DETECT_CHANGE_FREQUENCY(RUN_ID VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
COMMENT = 'Classifies column change frequency (FAST/SLOW/STATIC) for a profiling run. Reads source database from the run record.'
AS $$
import json as _json

def run(session, run_id: str) -> str:

    def esc(v):
        if v is None:
            return 'NULL'
        return "'" + str(v).replace("'", "''") + "'"

    s_run_id = run_id.replace("'", "''")

    fast_count   = 0
    slow_count   = 0
    static_count = 0
    snap_used    = False

    # ── Resolve run context ───────────────────────────────────────────────────

    run_info = session.sql(f"""
        SELECT SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE
        FROM META.DV_PROFILING_RUN
        WHERE RUN_ID = '{s_run_id}' AND STATUS = 'COMPLETED'
    """).collect()

    if not run_info:
        raise ValueError(f"No completed profiling run found for RUN_ID = {run_id}")

    source_database = run_info[0]['SOURCE_DATABASE'] or ''
    source_schema   = run_info[0]['SOURCE_SCHEMA']
    source_table    = run_info[0]['SOURCE_TABLE']

    if not source_database.strip():
        source_database = session.sql("SELECT CURRENT_DATABASE() AS D").collect()[0]['D']

    def qi(s):
        return '"' + s.replace('"', '""') + '"'

    q_db     = qi(source_database)
    s_schema = source_schema.replace("'",  "''")

    # ── Resolve PK columns from DV_PK_CANDIDATES ─────────────────────────────
    # Prefer modeler-confirmed; fall back to highest-scoring auto-detected

    pk_cols = []
    pk_rows = session.sql(f"""
        SELECT COLUMN_NAMES::VARCHAR AS COLS
        FROM META.DV_PK_CANDIDATES
        WHERE RUN_ID = '{s_run_id}'
        ORDER BY MODELER_SELECTED DESC, PK_SCORE DESC
        LIMIT 1
    """).collect()

    if pk_rows:
        try:
            pk_cols = _json.loads(pk_rows[0]['COLS'])
        except Exception:
            pk_cols = []

    # ── Strategy 1: Snapshot comparison ──────────────────────────────────────

    snap_table = None
    for suffix in ['_HIST', '_HISTORY', '_SNAP', '_SNAPSHOT', '_ARCHIVE']:
        candidate = source_table.upper() + suffix
        exists = session.sql(f"""
            SELECT COUNT(*) AS C
            FROM {q_db}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = UPPER('{s_schema}')
              AND TABLE_NAME   = '{candidate}'
        """).collect()[0][0]
        if exists:
            snap_table = candidate
            break

    if snap_table and pk_cols:
        # Find a date/timestamp column in the snapshot table
        date_cols = session.sql(f"""
            SELECT COLUMN_NAME
            FROM {q_db}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = UPPER('{s_schema}')
              AND TABLE_NAME   = '{snap_table}'
              AND DATA_TYPE    IN ('DATE','TIMESTAMP','TIMESTAMP_NTZ','TIMESTAMP_TZ','TIMESTAMP_LTZ')
            ORDER BY ORDINAL_POSITION
            LIMIT 1
        """).collect()

        if date_cols:
            snap_date_col = date_cols[0]['COLUMN_NAME']
            full_snap     = f'{q_db}.{qi(source_schema)}.{qi(snap_table)}'
            q_snap_dt     = qi(snap_date_col)

            # Two most recent snapshot dates
            snap_dates = session.sql(f"""
                SELECT DISTINCT {q_snap_dt}::DATE AS SNAP_DT
                FROM {full_snap}
                ORDER BY SNAP_DT DESC
                LIMIT 2
            """).collect()

            if len(snap_dates) >= 2:
                dt1 = str(snap_dates[0]['SNAP_DT'])
                dt2 = str(snap_dates[1]['SNAP_DT'])

                # Build PK join condition and SELECT list
                pk_select   = ", ".join(qi(c) for c in pk_cols)
                join_clause = " AND ".join(f"a.{qi(c)} = b.{qi(c)}" for c in pk_cols)

                profiled_cols = session.sql(f"""
                    SELECT COLUMN_NAME FROM META.DV_PROFILING_RESULTS
                    WHERE RUN_ID = '{s_run_id}'
                """).collect()

                for col_row in profiled_cols:
                    col_name = col_row['COLUMN_NAME']

                    # Skip PK columns themselves — always static by definition
                    if col_name.upper() in [c.upper() for c in pk_cols]:
                        session.sql(f"""
                            UPDATE META.DV_PROFILING_RESULTS
                            SET CHANGE_FREQUENCY = 'STATIC'
                            WHERE RUN_ID = '{s_run_id}' AND COLUMN_NAME = {esc(col_name)}
                        """).collect()
                        static_count += 1
                        continue

                    qcol = qi(col_name)

                    # Check column exists in snapshot table
                    col_in_snap = session.sql(f"""
                        SELECT COUNT(*) AS C
                        FROM {q_db}.INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = UPPER('{s_schema}')
                          AND TABLE_NAME   = '{snap_table}'
                          AND COLUMN_NAME  = UPPER('{col_name.replace("'","''")}')
                    """).collect()[0][0]

                    if not col_in_snap:
                        continue

                    # Compare values across snapshots using PK join
                    change_result = session.sql(f"""
                        SELECT
                            COUNT(*)                                                                    AS TOTAL_ROWS,
                            SUM(IFF(a.{qcol}::VARCHAR IS DISTINCT FROM b.{qcol}::VARCHAR, 1, 0))       AS CHANGED_ROWS
                        FROM
                            (SELECT {pk_select}, {qcol} FROM {full_snap} WHERE {q_snap_dt}::DATE = '{dt1}') a
                        INNER JOIN
                            (SELECT {pk_select}, {qcol} FROM {full_snap} WHERE {q_snap_dt}::DATE = '{dt2}') b
                          ON {join_clause}
                    """).collect()[0]

                    total_r    = change_result['TOTAL_ROWS']   or 0
                    changed_r  = change_result['CHANGED_ROWS'] or 0
                    change_pct = (changed_r / total_r * 100) if total_r > 0 else 0

                    if   change_pct > 20: freq = 'FAST';   fast_count   += 1
                    elif change_pct >= 1: freq = 'SLOW';   slow_count   += 1
                    else:                 freq = 'STATIC'; static_count += 1

                    session.sql(f"""
                        UPDATE META.DV_PROFILING_RESULTS
                        SET CHANGE_FREQUENCY = {esc(freq)}
                        WHERE RUN_ID = '{s_run_id}' AND COLUMN_NAME = {esc(col_name)}
                    """).collect()

                snap_used = True

    # ── Strategy 2: Semantic classification ──────────────────────────────────
    # Applies to columns still NULL (no snapshot, no PK, or column absent in snap)

    unclassified = session.sql(f"""
        SELECT COLUMN_NAME FROM META.DV_PROFILING_RESULTS
        WHERE RUN_ID = '{s_run_id}' AND CHANGE_FREQUENCY IS NULL
    """).collect()

    # FAST — values expected to change frequently
    fast_suffixes = (
        '_AMT',   '_BAL',    '_RATE',   '_QTY',    '_CNT',    '_COUNT',
        '_STAT',  '_STATUS', '_FLG',    '_FLAG',   '_PRC',    '_PRICE',
        '_PCT',   '_PERCENT','_TTL',    '_TOTAL',
        # demographics — frequently updated scores / assessments
        '_SCORE', '_RATING', '_INDEX',  '_RANK',   '_RISK',
        '_INCOME','_SALARY', '_EARN',   '_WAGES',
        '_EMPLOY','_JOB',    '_OCCUP',  '_WORK',
    )

    # SLOW — attributes that change infrequently
    slow_suffixes = (
        '_NM',    '_NAME',   '_ADDR',   '_ADDRESS','_TYP',    '_TYPE',
        '_CD',    '_CODE',   '_DESCR',  '_DESC',   '_LBL',    '_LABEL',
        '_CTGY',  '_CATG',   '_CATEGORY',
        # demographics — stable personal/geographic attributes
        '_RACE',  '_ETHNIC', '_GENDER', '_SEX',    '_MARITAL','_MRTL',
        '_EDUC',  '_SCHOOL', '_LANG',   '_LANGUAGE','_RELIG', '_NATION',
        '_CITIZEN','_ZIP',   '_POSTAL', '_STATE',  '_CITY',   '_COUNTY',
        '_REGION','_DISTRICT','_METRO',
    )

    # STATIC — identifiers and immutable facts
    static_keywords = (
        '_ID',    '_KEY',    '_NBR',    '_NUM',    '_SSN',    '_TIN',
        '_DOB',   'BIRTH',   '_OPEN_DT','_CREAT',  '_SETUP',  '_INIT',
        '_ORIG',
        # demographics — immutable identifiers / birth facts
        '_SIN',   '_NIN',    '_PASSPORT','_BORN',  '_BIRTH_DT',
    )

    for col_row in unclassified:
        col_name = col_row['COLUMN_NAME']
        cu       = col_name.upper()

        if   any(cu.endswith(s) for s in fast_suffixes):
            freq = 'FAST';   fast_count   += 1
        elif any(cu.endswith(s) for s in slow_suffixes):
            freq = 'SLOW';   slow_count   += 1
        elif any(s in cu for s in static_keywords):
            freq = 'STATIC'; static_count += 1
        else:
            freq = 'SLOW';   slow_count   += 1  # conservative default

        session.sql(f"""
            UPDATE META.DV_PROFILING_RESULTS
            SET CHANGE_FREQUENCY = {esc(freq)}
            WHERE RUN_ID = '{s_run_id}' AND COLUMN_NAME = {esc(col_name)}
        """).collect()

    method = 'snapshot comparison (PK join)' if snap_used else 'semantic classification'
    return (f"Change frequency classified via {method}: "
            f"FAST={fast_count}, SLOW={slow_count}, STATIC={static_count}")

$$;

-- ── Grant ─────────────────────────────────────────────────────────────────────
-- GRANT USAGE ON PROCEDURE META.SP_DETECT_CHANGE_FREQUENCY(VARCHAR) TO ROLE NEXUS_MODELER;

-- ── Quick test ────────────────────────────────────────────────────────────────
-- CALL META.SP_DETECT_CHANGE_FREQUENCY('your-run-id');
