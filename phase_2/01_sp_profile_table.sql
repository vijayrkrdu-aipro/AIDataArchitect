-- ============================================================================
-- NEXUS DV2.0 — Phase 2: SP_PROFILE_TABLE
-- Profiles a source table column-by-column and writes results to
-- META.DV_PROFILING_RESULTS. Works against ANY database accessible to
-- the Snowflake role — not limited to NEXUS.
-- Automatically uses HLL for tables >10M rows.
--
-- Parameters:
--   SOURCE_DATABASE  — database where the table lives (NULL = current database)
--   SOURCE_SCHEMA    — schema name
--   SOURCE_TABLE     — table name
--   SOURCE_SYSTEM    — optional short code for the source system (e.g. ACCT_SYS).
--                      If NULL/blank, defaults to the database name.
--                      Used in satellite naming: SAT_CUSTOMER_DETAILS__<SOURCE_SYSTEM>
--   RUN_ID           — optional. UUID generated automatically if not supplied.
--
-- Call:
--   CALL META.SP_PROFILE_TABLE('STAGING', 'ACCT_MSTR');                              -- minimal
--   CALL META.SP_PROFILE_TABLE('STAGING', 'ACCT_MSTR', 'NEXUS', 'ACCT_SYS', NULL);  -- explicit all
--   CALL META.SP_PROFILE_TABLE('CORE', 'CUSTOMERS', 'PROD_DB', 'CRM_SYS', NULL);    -- other database
-- Returns: the run_id (UUID string)
-- ============================================================================

USE SCHEMA NEXUS.META;

CREATE OR REPLACE PROCEDURE META.SP_PROFILE_TABLE(
    SOURCE_SCHEMA   VARCHAR,
    SOURCE_TABLE    VARCHAR,
    SOURCE_DATABASE VARCHAR DEFAULT NULL,
    SOURCE_SYSTEM   VARCHAR DEFAULT NULL,
    RUN_ID          VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
COMMENT = 'Profiles any accessible Snowflake table. SOURCE_DATABASE and SOURCE_SYSTEM are optional.'
AS $$
import uuid
import json
import re

def run(session,
        source_schema: str = '',
        source_table: str = '',
        source_database: str = None,
        source_system: str = None,
        run_id: str = None) -> str:

    # ── Helpers ──────────────────────────────────────────────────────────────

    def esc(v):
        """Escape a value for a SQL single-quoted string literal."""
        if v is None:
            return 'NULL'
        return "'" + str(v).replace("'", "''") + "'"

    def num(v):
        """Return SQL-safe numeric literal or NULL."""
        return str(v) if v is not None else 'NULL'

    # ── Resolve optional parameters ───────────────────────────────────────────

    # Database: default to current database if not provided
    if not source_database or not source_database.strip():
        source_database = session.sql("SELECT CURRENT_DATABASE() AS D").collect()[0]['D']

    # Source system: default to sanitised database name if not provided
    if not source_system or not source_system.strip():
        source_system = re.sub(r'[^A-Z0-9]', '_', source_database.upper().strip())

    if not run_id or not run_id.strip():
        run_id = str(uuid.uuid4())

    # ── Quoted identifiers (3-part: database.schema.table) ───────────────────

    def qi(s):
        return '"' + s.replace('"', '""') + '"'

    q_db     = qi(source_database)
    q_schema = qi(source_schema)
    q_table  = qi(source_table)
    full_tbl = f'{q_db}.{q_schema}.{q_table}'

    # SQL-safe string values for single-quoted literals
    s_db     = source_database.replace("'", "''")
    s_schema = source_schema.replace("'",   "''")
    s_table  = source_table.replace("'",    "''")
    s_system = source_system.replace("'",   "''")
    s_run_id = run_id.replace("'",          "''")

    # ── Begin ─────────────────────────────────────────────────────────────────

    try:
        session.sql(f"""
            INSERT INTO META.DV_PROFILING_RUN
                (RUN_ID, SOURCE_DATABASE, SOURCE_SYSTEM, SOURCE_SCHEMA, SOURCE_TABLE,
                 STATUS, STARTED_AT, PROFILED_BY)
            SELECT '{s_run_id}', '{s_db}', '{s_system}', '{s_schema}', '{s_table}',
                   'RUNNING', CURRENT_TIMESTAMP(), CURRENT_USER()
        """).collect()

        # Row count → EXACT vs HLL
        row_count = session.sql(f"SELECT COUNT(*) FROM {full_tbl}").collect()[0][0]
        use_hll   = (row_count > 10_000_000)
        method    = 'HLL' if use_hll else 'EXACT'

        # Column metadata — cross-database INFORMATION_SCHEMA
        columns = session.sql(f"""
            SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION
            FROM {q_db}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = UPPER('{s_schema}')
              AND TABLE_NAME   = UPPER('{s_table}')
            ORDER BY ORDINAL_POSITION
        """).collect()

        if not columns:
            raise ValueError(
                f"No columns found for {source_database}.{source_schema}.{source_table}. "
                "Verify the table exists and the role has SELECT privilege."
            )

        col_count = len(columns)

        session.sql(f"""
            UPDATE META.DV_PROFILING_RUN
            SET ROW_COUNT = {row_count}, COLUMN_COUNT = {col_count}, PROFILING_METHOD = '{method}'
            WHERE RUN_ID = '{s_run_id}'
        """).collect()

        # ── Profile each column ──────────────────────────────────────────────

        uuid_pat  = "'^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$'"
        email_pat = "'^[^@ ]+@[^@ ]+[.][^@ ]+$'"
        num_pat   = "'^[0-9]+$'"
        code_pat  = "'^[A-Z]{2,5}$'"

        for col_info in columns:
            col_name     = col_info['COLUMN_NAME']
            source_dtype = col_info['DATA_TYPE']
            ordinal      = col_info['ORDINAL_POSITION']

            qcol         = qi(col_name)
            cast_col     = f'{qcol}::VARCHAR'
            distinct_exp = f'HLL({qcol})' if use_hll else f'COUNT(DISTINCT {qcol})'
            non_null_exp = f'COUNT_IF({qcol} IS NOT NULL)'

            stats = session.sql(f"""
                SELECT
                    {distinct_exp}                                                           AS DISTINCT_COUNT,
                    COUNT_IF({qcol} IS NULL)                                                 AS NULL_COUNT,
                    MIN(LENGTH({cast_col}))                                                  AS MIN_LEN,
                    MAX(LENGTH({cast_col}))                                                  AS MAX_LEN,
                    ROUND(AVG(LENGTH({cast_col})), 2)                                        AS AVG_LEN,
                    LEFT(MIN({cast_col}), 500)                                               AS MIN_VAL,
                    LEFT(MAX({cast_col}), 500)                                               AS MAX_VAL,
                    CASE
                        WHEN SUM(IFF(TRY_CAST({cast_col} AS NUMBER)        IS NOT NULL, 1, 0))
                             / NULLIF({non_null_exp}, 0) >= 0.95 THEN 'NUMBER'
                        WHEN SUM(IFF(TRY_CAST({cast_col} AS TIMESTAMP_NTZ) IS NOT NULL, 1, 0))
                             / NULLIF({non_null_exp}, 0) >= 0.95 THEN 'TIMESTAMP_NTZ'
                        WHEN SUM(IFF(TRY_CAST({cast_col} AS DATE)          IS NOT NULL, 1, 0))
                             / NULLIF({non_null_exp}, 0) >= 0.95 THEN 'DATE'
                        WHEN SUM(IFF(LOWER({cast_col}) IN ('true','false','yes','no','1','0','y','n'), 1, 0))
                             / NULLIF({non_null_exp}, 0) >= 0.95 THEN 'BOOLEAN'
                        ELSE 'VARCHAR'
                    END                                                                      AS INFERRED_TYPE,
                    CASE
                        WHEN SUM(IFF(REGEXP_LIKE({cast_col}, {uuid_pat}),  1, 0)) / NULLIF({non_null_exp}, 0) >= 0.80 THEN 'UUID'
                        WHEN SUM(IFF(REGEXP_LIKE({cast_col}, {email_pat}), 1, 0)) / NULLIF({non_null_exp}, 0) >= 0.80 THEN 'EMAIL'
                        WHEN SUM(IFF(REGEXP_LIKE({cast_col}, {num_pat}),   1, 0)) / NULLIF({non_null_exp}, 0) >= 0.95 THEN 'NUMERIC_CODE'
                        WHEN SUM(IFF(REGEXP_LIKE({cast_col}, {code_pat}),  1, 0)) / NULLIF({non_null_exp}, 0) >= 0.95 THEN 'SHORT_CODE'
                        ELSE NULL
                    END                                                                      AS PATTERN
                FROM {full_tbl}
            """).collect()[0]

            distinct_count   = int(stats['DISTINCT_COUNT']) if stats['DISTINCT_COUNT'] is not None else 0
            null_count       = int(stats['NULL_COUNT'])     if stats['NULL_COUNT']     is not None else 0
            uniqueness_ratio = round(distinct_count / row_count, 6)      if row_count > 0 else 0.0
            null_pct         = round(null_count     / row_count * 100, 4) if row_count > 0 else 0.0
            is_pk_candidate  = (uniqueness_ratio >= 0.95 and null_pct == 0.0)

            cu = col_name.upper()
            if   any(cu.endswith(x) for x in ['_AMT','_BAL','_RATE','_QTY','_CNT','_COUNT','_STAT','_STATUS','_FLG','_FLAG']):
                change_freq = 'FAST'
            elif any(cu.endswith(x) for x in ['_NM','_NAME','_ADDR','_ADDRESS','_TYP','_TYPE','_CD','_CODE','_DESCR','_DESC']):
                change_freq = 'SLOW'
            elif any(s in cu for s in ['_ID','_KEY','_NBR','_NUM','_SSN','_TIN','DOB','_BIRTH','_OPEN_DT','_CREATE_DT','_SETUP_DT']):
                change_freq = 'STATIC'
            else:
                change_freq = None

            top_rows   = session.sql(f"""
                SELECT LEFT({cast_col}, 200) AS V, COUNT(*) AS CNT
                FROM {full_tbl}
                WHERE {qcol} IS NOT NULL
                GROUP BY {cast_col}
                ORDER BY CNT DESC
                LIMIT 5
            """).collect()
            # Use ARRAY_CONSTRUCT so each value is individually SQL-escaped,
            # avoiding PARSE_JSON failures on values with special characters.
            if top_rows:
                arr_vals = ', '.join(
                    'NULL' if r['V'] is None else esc(str(r['V']))
                    for r in top_rows
                )
                top_values_sql = f'ARRAY_CONSTRUCT({arr_vals})'
            else:
                top_values_sql = "TO_VARIANT('[]')"

            session.sql(f"""
                INSERT INTO META.DV_PROFILING_RESULTS (
                    RUN_ID, COLUMN_NAME, ORDINAL_POSITION,
                    SOURCE_DATA_TYPE, INFERRED_DATA_TYPE,
                    ROW_COUNT, DISTINCT_COUNT, UNIQUENESS_RATIO,
                    NULL_COUNT, NULL_PERCENTAGE,
                    MIN_LENGTH, MAX_LENGTH, AVG_LENGTH,
                    MIN_VALUE, MAX_VALUE,
                    TOP_VALUES, PATTERN_DETECTED,
                    CHANGE_FREQUENCY, IS_PK_CANDIDATE
                )
                SELECT
                    {esc(run_id)}, {esc(col_name)}, {num(ordinal)},
                    {esc(source_dtype)}, {esc(stats['INFERRED_TYPE'])},
                    {num(row_count)}, {num(distinct_count)}, {num(uniqueness_ratio)},
                    {num(null_count)}, {num(null_pct)},
                    {num(stats['MIN_LEN'])}, {num(stats['MAX_LEN'])}, {num(stats['AVG_LEN'])},
                    {esc(stats['MIN_VAL'])}, {esc(stats['MAX_VAL'])},
                    {top_values_sql},
                    {esc(stats['PATTERN'])},
                    {esc(change_freq)}, {'TRUE' if is_pk_candidate else 'FALSE'}
            """).collect()

        # ── Finalize ──────────────────────────────────────────────────────────

        session.sql(f"""
            UPDATE META.DV_PROFILING_RUN
            SET STATUS = 'COMPLETED', COMPLETED_AT = CURRENT_TIMESTAMP()
            WHERE RUN_ID = '{s_run_id}'
        """).collect()

        session.sql(f"""
            INSERT INTO META.DV_AUDIT_LOG
                (ACTION_TYPE, ENTITY_TYPE, ENTITY_ID, SOURCE_TABLE, SOURCE_SYSTEM, ACTION_DETAILS)
            SELECT 'PROFILE', 'RUN', {esc(run_id)}, {esc(source_table)}, {esc(source_system)},
                   PARSE_JSON({esc(json.dumps({
                       "run_id":           run_id,
                       "source_database":  source_database,
                       "columns_profiled": col_count,
                       "row_count":        row_count,
                       "method":           method
                   }))})
        """).collect()

        return run_id

    except Exception as exc:
        err = str(exc)[:1990].replace("'", "''")
        try:
            session.sql(f"""
                UPDATE META.DV_PROFILING_RUN
                SET STATUS = 'FAILED',
                    ERROR_MESSAGE = '{err}',
                    COMPLETED_AT  = CURRENT_TIMESTAMP()
                WHERE RUN_ID = '{s_run_id}'
            """).collect()
        except Exception:
            pass
        raise

$$;

-- ── Grant ─────────────────────────────────────────────────────────────────────
-- GRANT USAGE ON PROCEDURE META.SP_PROFILE_TABLE(VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR)
--   TO ROLE NEXUS_MODELER;

-- ── Quick tests ───────────────────────────────────────────────────────────────
-- Profile a table in the current database (minimal):
--   CALL META.SP_PROFILE_TABLE('STAGING', 'ACCT_MSTR');
--
-- Profile a table in another database with an explicit source system:
--   CALL META.SP_PROFILE_TABLE('CORE', 'CUSTOMERS', 'PROD_DB', 'CRM_SYS', NULL);
