-- ============================================================================
-- NEXUS DV2.0 — Phase 2: SP_DETECT_CHANGE_FREQUENCY
-- Updates CHANGE_FREQUENCY in META.DV_PROFILING_RESULTS for a given run.
-- Works against any database — reads SOURCE_DATABASE from the profiling run.
--
-- Strategy (priority order):
--   1. Snapshot comparison — looks for companion _HIST/_SNAP table in the
--      same database.schema, with a date/timestamp column
--   2. Semantic classification — column name pattern matching (fallback)
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

    # ── Resolve run context (reads database from the run record) ─────────────

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

    # If SOURCE_DATABASE was NULL (pre-migration rows), fall back to current db
    if not source_database.strip():
        source_database = session.sql("SELECT CURRENT_DATABASE() AS D").collect()[0]['D']

    def qi(s):
        return '"' + s.replace('"', '""') + '"'

    q_db     = qi(source_database)
    s_schema = source_schema.replace("'",  "''")
    s_db     = source_database.replace("'","''")

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

    if snap_table:
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

                profiled_cols = session.sql(f"""
                    SELECT COLUMN_NAME FROM META.DV_PROFILING_RESULTS
                    WHERE RUN_ID = '{s_run_id}'
                """).collect()

                for col_row in profiled_cols:
                    col_name = col_row['COLUMN_NAME']
                    qcol     = qi(col_name)

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

                    # Compare values across the two snapshots
                    change_result = session.sql(f"""
                        SELECT
                            COUNT(*)                                                          AS TOTAL_ROWS,
                            SUM(IFF(a.{qcol}::VARCHAR != b.{qcol}::VARCHAR, 1, 0))           AS CHANGED_ROWS
                        FROM
                            (SELECT {qcol} FROM {full_snap} WHERE {q_snap_dt}::DATE = '{dt1}') a
                        FULL OUTER JOIN
                            (SELECT {qcol} FROM {full_snap} WHERE {q_snap_dt}::DATE = '{dt2}') b
                          ON a.{qcol}::VARCHAR = b.{qcol}::VARCHAR
                    """).collect()[0]

                    total_r   = change_result['TOTAL_ROWS']   or 0
                    changed_r = change_result['CHANGED_ROWS'] or 0
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
    # Applies to all columns still NULL (snapshot not available or column absent)

    unclassified = session.sql(f"""
        SELECT COLUMN_NAME FROM META.DV_PROFILING_RESULTS
        WHERE RUN_ID = '{s_run_id}' AND CHANGE_FREQUENCY IS NULL
    """).collect()

    fast_suffixes   = ('_AMT','_BAL','_RATE','_QTY','_CNT','_COUNT','_STAT','_STATUS',
                       '_FLG','_FLAG','_PRC','_PRICE','_PCT','_PERCENT','_TTL','_TOTAL')
    slow_suffixes   = ('_NM','_NAME','_ADDR','_ADDRESS','_TYP','_TYPE','_CD','_CODE',
                       '_DESCR','_DESC','_LBL','_LABEL','_CTGY','_CATG','_CATEGORY')
    static_keywords = ('_ID','_KEY','_NBR','_NUM','_SSN','_TIN','_DOB','BIRTH',
                       '_OPEN_DT','_CREAT','_SETUP','_INIT','_ORIG')

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
            freq = 'SLOW';   slow_count   += 1  # default: assume slow-changing

        session.sql(f"""
            UPDATE META.DV_PROFILING_RESULTS
            SET CHANGE_FREQUENCY = {esc(freq)}
            WHERE RUN_ID = '{s_run_id}' AND COLUMN_NAME = {esc(col_name)}
        """).collect()

    method = 'snapshot comparison' if snap_used else 'semantic classification'
    return (f"Change frequency classified via {method}: "
            f"FAST={fast_count}, SLOW={slow_count}, STATIC={static_count}")

$$;

-- ── Grant ─────────────────────────────────────────────────────────────────────
-- GRANT USAGE ON PROCEDURE META.SP_DETECT_CHANGE_FREQUENCY(VARCHAR) TO ROLE NEXUS_MODELER;

-- ── Quick test ────────────────────────────────────────────────────────────────
-- CALL META.SP_DETECT_CHANGE_FREQUENCY('your-run-id');
