# ============================================================================
# NEXUS DV2.0 — Streamlit in Snowflake App (Phases 2 & 3)
# Pages:
#   1. Source Tables  — browse any accessible database/schema/table,
#                       trigger profiling, manage runs
#   2. Profiling Review — per-column stats, PK candidates, change frequency
#   3. Design Workbench — AI-assisted DV2.0 modeling, editing, diagram
# ============================================================================

import streamlit as st
import pandas as pd
import uuid
import json
import re
from snowflake.snowpark.context import get_active_session
import snowflake.snowpark.functions as F

# ── Page config & session ─────────────────────────────────────────────────────

st.set_page_config(page_title="NEXUS DV2.0", layout="wide")
session = get_active_session()

# ── Navigation ────────────────────────────────────────────────────────────────

st.sidebar.title("NEXUS DV2.0")
st.sidebar.caption("Data Vault Automation Platform")
st.sidebar.markdown("---")

st.sidebar.markdown(
    "<p style='font-size:22px;font-weight:800;margin:0 0 6px 0;'>🏗️ Raw Vault</p>",
    unsafe_allow_html=True
)
page = st.sidebar.radio("",
    ["Identify Source", "Profile and Review", "Design Raw Vault", "Generate Erwin", "Generate DBT"],
    key="nav", label_visibility="collapsed")

# ── Cached data loaders ───────────────────────────────────────────────────────

@st.cache_data(ttl=120)
def get_databases():
    """Return all databases accessible to the current role, excluding system dbs."""
    rows = session.sql("SHOW DATABASES").collect()
    exclude = {'SNOWFLAKE', 'SNOWFLAKE_SAMPLE_DATA'}
    return sorted([r['name'] for r in rows if r['name'] not in exclude])

@st.cache_data(ttl=60)
def get_schemas(database: str):
    """Return user-facing schemas in a given database."""
    rows = session.sql(f"""
        SELECT SCHEMA_NAME
        FROM "{database}".INFORMATION_SCHEMA.SCHEMATA
        WHERE SCHEMA_NAME != 'INFORMATION_SCHEMA'
        ORDER BY SCHEMA_NAME
    """).collect()
    return [r['SCHEMA_NAME'] for r in rows]

@st.cache_data(ttl=60)
def get_tables(database: str, schema: str):
    """Return base tables and views in a given database.schema."""
    rows = session.sql(f"""
        SELECT TABLE_NAME, TABLE_TYPE
        FROM "{database}".INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema.replace("'","''")}' AND TABLE_TYPE IN ('BASE TABLE','VIEW')
        ORDER BY TABLE_TYPE, TABLE_NAME
    """).collect()
    # Prefix views with a marker so modelers can distinguish them
    return [
        ("⬡ " if r['TABLE_TYPE'] == 'VIEW' else "") + r['TABLE_NAME']
        for r in rows
    ]

@st.cache_data(ttl=60)
def get_columns(database: str, schema: str, table: str):
    """Return ordered column names for a given table."""
    rows = session.sql(f"""
        SELECT COLUMN_NAME
        FROM "{database}".INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema.replace("'","''")}' AND TABLE_NAME = '{table.replace("'","''")}'
        ORDER BY ORDINAL_POSITION
    """).collect()
    return [r['COLUMN_NAME'] for r in rows]

# ── Ensure internal persistence tables exist ─────────────────────────────────

@st.cache_resource
def _ensure_meta_tables():
    """Create META.DV_COLUMN_DEFINITIONS if it doesn't exist yet."""
    session.sql("""
        CREATE TABLE IF NOT EXISTS META.DV_COLUMN_DEFINITIONS (
            SOURCE_SCHEMA     VARCHAR(100)  NOT NULL,
            SOURCE_TABLE      VARCHAR(100)  NOT NULL,
            COLUMN_NAME       VARCHAR(100)  NOT NULL,
            DEFINITION        TEXT,
            IS_SENSITIVE      VARCHAR(20)   DEFAULT 'None',
            TABLE_DESCRIPTION TEXT,
            SAVED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            SAVED_BY          VARCHAR(100)  DEFAULT CURRENT_USER(),
            CONSTRAINT PK_DV_COL_DEFS PRIMARY KEY (SOURCE_SCHEMA, SOURCE_TABLE, COLUMN_NAME)
        )
    """).collect()

_ensure_meta_tables()

# ── Shared session state ──────────────────────────────────────────────────────

for k, v in {"sel_run_id": None, "sel_table": None}.items():
    if k not in st.session_state:
        st.session_state[k] = v


def _src_system_badge(source_system: str):
    """Render a small greyed-out source system pill."""
    if source_system and source_system != "—":
        st.markdown(
            f"<span style='background:#f3f4f6;color:#6b7280;border:1px solid #e5e7eb;"
            f"border-radius:12px;padding:2px 10px;font-size:12px;font-weight:600;"
            f"letter-spacing:0.5px'>"
            f"SOURCE SYSTEM &nbsp;{source_system.upper()}</span>",
            unsafe_allow_html=True
        )

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 1: SOURCE TABLES
# ══════════════════════════════════════════════════════════════════════════════

def page_source_tables():
    st.title("Source Tables")

    col_new, col_runs = st.columns([1, 2])

    # ── Left: profile a table ─────────────────────────────────────────────────

    with col_new:
        st.subheader("Profile a Table / View")
        st.caption("Choose from any accessible database. Views are marked ⬡.")

        # Cascading selectors — outside a form so they react to each other
        databases = get_databases()
        if not databases:
            st.error("No accessible databases found.")
            return

        source_db = st.selectbox("Database", databases, key="new_db")

        schemas = get_schemas(source_db) if source_db else []
        source_schema = st.selectbox("Schema", schemas, key="new_schema",
                                     disabled=not schemas)

        tables = get_tables(source_db, source_schema) if source_db and source_schema else []
        source_tables_raw = st.multiselect("Tables / Views", tables, key="new_table",
                                           disabled=not tables)
        # Strip the view prefix marker before using names in SQL / SP calls
        source_tables = [t.lstrip("⬡ ") for t in source_tables_raw]
        source_table  = source_tables[0] if len(source_tables) == 1 else None

        # PK suggestion only available for single-table profiling
        suggested_pk_cols = []
        if len(source_tables) == 1:
            try:
                all_cols = get_columns(source_db, source_schema, source_table)
                suggested_pk_cols = st.multiselect(
                    "Suggest Primary Key column(s) *(optional)*",
                    options=all_cols,
                    key="new_pk_cols",
                    help=(
                        "Select one or more columns you believe form the primary key. "
                        "The profiler will compute composite uniqueness and null coverage "
                        "for your selection and record it alongside auto-detected candidates."
                    )
                )
            except Exception:
                pass
        elif len(source_tables) > 1:
            st.caption(f"{len(source_tables)} tables selected — PK suggestion available for single-table profiling only.")

        st.markdown("---")
        source_system = st.text_input(
            "Source System Code *(required, max 5 chars)*",
            key="new_system",
            placeholder="e.g. ACCTS",
            help=(
                "A short code (up to 5 characters) identifying the source system. "
                "Used as the prefix in DV2.0 satellite naming: "
                "SAT_ACCTS_CUSTOMER_DETAILS. One source system may span multiple databases."
            )
        )
        _sys_val   = source_system.strip().upper()
        _sys_valid = bool(_sys_val) and len(_sys_val) <= 5
        if _sys_val and len(_sys_val) > 5:
            st.error("Source system code must be 5 characters or fewer.")
        elif not _sys_val:
            st.caption("⚠ Source system code is required before profiling.")

        st.markdown("")
        run_btn = st.button("▶ Run Profiling", use_container_width=True,
                            disabled=not (source_db and source_schema and source_tables and _sys_valid))

        if run_btn:
            effective_system = _sys_val
            last_run_id, last_table = None, None
            total = len(source_tables)
            progress_bar = st.progress(0, text="Starting…")

            for i, tbl in enumerate(source_tables):
                new_run_id = str(uuid.uuid4())
                progress_bar.progress(i / total, text=f"Profiling {tbl} ({i+1}/{total})…")
                try:
                    session.call("META.SP_PROFILE_TABLE",
                                 source_schema, tbl,
                                 source_db,
                                 effective_system,
                                 new_run_id,
                                 json.dumps(suggested_pk_cols) if (suggested_pk_cols and total == 1) else None)
                    session.call("META.SP_DETECT_PK_CANDIDATES", new_run_id)
                    session.call("META.SP_DETECT_CHANGE_FREQUENCY", new_run_id)
                    last_run_id, last_table = new_run_id, tbl
                    st.success(f"✅ {tbl} complete.")
                except Exception as e:
                    st.error(f"❌ {tbl} failed: {e}")

            progress_bar.progress(1.0, text="Done.")
            if last_run_id:
                st.session_state.sel_run_id = last_run_id
                st.session_state.sel_table  = last_table
                get_databases.clear()
                st.experimental_rerun()

    # ── Right: previous profiling runs (single-table selection only) ─────────

    with col_runs:
        if len(source_tables) == 1 and source_table:
            st.subheader("Previous Profiling Runs")

            safe_table  = source_table.replace("'", "''")
            safe_schema = source_schema.replace("'", "''")
            safe_db     = source_db.replace("'", "''")

            runs_df = session.sql(f"""
                WITH LATEST_SYS AS (
                    SELECT FIRST_VALUE(SOURCE_SYSTEM) OVER (
                               ORDER BY STARTED_AT DESC) AS CURRENT_SYSTEM
                    FROM META.DV_PROFILING_RUN
                    WHERE SOURCE_TABLE  = '{safe_table}'
                      AND SOURCE_SCHEMA = '{safe_schema}'
                      AND COALESCE(SOURCE_DATABASE, '') = '{safe_db}'
                      AND STATUS = 'COMPLETED'
                    LIMIT 1
                )
                SELECT
                    RUN_ID,
                    COALESCE(SOURCE_DATABASE, '—')               AS SRC_DATABASE,
                    SOURCE_SYSTEM,
                    SOURCE_SCHEMA                                AS SRC_SCHEMA,
                    SOURCE_TABLE                                 AS SRC_TABLE,
                    ROW_COUNT,
                    COLUMN_COUNT                                 AS COLS,
                    PROFILING_METHOD                             AS METHOD,
                    STATUS,
                    TO_CHAR(STARTED_AT, 'YYYY-MM-DD HH24:MI')   AS PROFILED_AT
                FROM META.DV_PROFILING_RUN
                WHERE SOURCE_TABLE  = '{safe_table}'
                  AND SOURCE_SCHEMA = '{safe_schema}'
                  AND COALESCE(SOURCE_DATABASE, '') = '{safe_db}'
                  AND SOURCE_SYSTEM = (SELECT CURRENT_SYSTEM FROM LATEST_SYS)
                ORDER BY STARTED_AT DESC
                LIMIT 100
            """).to_pandas()

            if runs_df.empty:
                st.info(f"No profiling runs yet for {source_db}.{source_schema}.{source_table}.")
            else:
                icon = {"COMPLETED": "✅", "RUNNING": "⏳", "FAILED": "❌"}
                runs_df["STATUS"] = runs_df["STATUS"].map(lambda s: f"{icon.get(s,'')} {s}")

                st.dataframe(
                    runs_df[["SRC_DATABASE","SOURCE_SYSTEM","SRC_SCHEMA","SRC_TABLE","ROW_COUNT","COLS","METHOD","STATUS","PROFILED_AT"]],
                    use_container_width=True
                )

                completed = runs_df[runs_df["STATUS"].str.contains("✅")]
                if not completed.empty:
                    labels = (
                        completed["SRC_DATABASE"] + "." +
                        completed["SRC_SCHEMA"] + "." +
                        completed["SRC_TABLE"] + "  [" +
                        completed["SOURCE_SYSTEM"] + "]"
                    ).tolist()
                    chosen = st.selectbox("Select run to review:", labels, key="runs_sel")
                    if st.button("Open in Profiling Review →", key="runs_go"):
                        idx = labels.index(chosen)
                        row = completed.iloc[idx]
                        st.session_state.sel_run_id = row["RUN_ID"]
                        st.session_state.sel_table  = row["SRC_TABLE"]
                        st.session_state.nav = "Profile and Review"
                        st.experimental_rerun()


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 2: PROFILING REVIEW
# ══════════════════════════════════════════════════════════════════════════════

def page_profiling_review():
    st.title("Profiling Review")

    # ── Cascading selectors: Database → Schema → Table → Run ─────────────────

    all_runs = session.sql("""
        WITH LATEST_SYS AS (
            SELECT SOURCE_SCHEMA, SOURCE_TABLE,
                   FIRST_VALUE(SOURCE_SYSTEM) OVER (
                       PARTITION BY SOURCE_SCHEMA, SOURCE_TABLE
                       ORDER BY STARTED_AT DESC
                   ) AS CURRENT_SYSTEM
            FROM META.DV_PROFILING_RUN
            WHERE STATUS = 'COMPLETED'
        )
        SELECT DISTINCT
            r.RUN_ID,
            COALESCE(r.SOURCE_DATABASE,'—') AS SRC_DATABASE,
            r.SOURCE_SCHEMA, r.SOURCE_TABLE, r.SOURCE_SYSTEM,
            TO_CHAR(r.STARTED_AT,'YYYY-MM-DD HH24:MI') AS RUN_DATE
        FROM META.DV_PROFILING_RUN r
        JOIN LATEST_SYS ls
          ON ls.SOURCE_SCHEMA  = r.SOURCE_SCHEMA
         AND ls.SOURCE_TABLE   = r.SOURCE_TABLE
         AND ls.CURRENT_SYSTEM = r.SOURCE_SYSTEM
        WHERE r.STATUS = 'COMPLETED'
        ORDER BY RUN_DATE DESC
        LIMIT 200
    """).to_pandas()

    if all_runs.empty:
        st.info("No completed profiling runs. Go to Source Tables and profile a table first.")
        return

    sel_c1, sel_c2, sel_c3 = st.columns(3)

    with sel_c1:
        dbs = sorted(all_runs["SRC_DATABASE"].unique().tolist())
        # Pre-select db from session state if possible
        presel_db = None
        if st.session_state.sel_run_id:
            match = all_runs[all_runs["RUN_ID"] == st.session_state.sel_run_id]
            if not match.empty:
                presel_db = match.iloc[0]["SRC_DATABASE"]
        db_idx = dbs.index(presel_db) if presel_db in dbs else 0
        sel_db = st.selectbox("Database", dbs, index=db_idx, key="pr_db")

    db_runs = all_runs[all_runs["SRC_DATABASE"] == sel_db]

    with sel_c2:
        schemas = sorted(db_runs["SOURCE_SCHEMA"].unique().tolist())
        presel_schema = None
        if st.session_state.sel_run_id:
            match = db_runs[db_runs["RUN_ID"] == st.session_state.sel_run_id]
            if not match.empty:
                presel_schema = match.iloc[0]["SOURCE_SCHEMA"]
        sc_idx = schemas.index(presel_schema) if presel_schema in schemas else 0
        sel_schema = st.selectbox("Schema", schemas, index=sc_idx, key="pr_schema")

    schema_runs = db_runs[db_runs["SOURCE_SCHEMA"] == sel_schema]

    with sel_c3:
        tables = sorted(schema_runs["SOURCE_TABLE"].unique().tolist())
        presel_table = None
        if st.session_state.sel_run_id:
            match = schema_runs[schema_runs["RUN_ID"] == st.session_state.sel_run_id]
            if not match.empty:
                presel_table = match.iloc[0]["SOURCE_TABLE"]
        tbl_idx = tables.index(presel_table) if presel_table in tables else 0
        sel_table = st.selectbox("Table", tables, index=tbl_idx, key="pr_table")

    table_runs = schema_runs[schema_runs["SOURCE_TABLE"] == sel_table]
    run_labels = (table_runs["RUN_DATE"] + "  [" + table_runs["SOURCE_SYSTEM"] + "]").tolist()

    presel_run_idx = 0
    if st.session_state.sel_run_id:
        match_idx = table_runs[table_runs["RUN_ID"] == st.session_state.sel_run_id].index.tolist()
        if match_idx:
            presel_run_idx = table_runs.index.tolist().index(match_idx[0])

    sel_run_label = st.selectbox("Profiling run:", run_labels, index=presel_run_idx, key="pr_run")
    run_id = table_runs.iloc[run_labels.index(sel_run_label)]["RUN_ID"]

    info_quick = session.sql(f"""
        SELECT PROFILING_METHOD, ROW_COUNT, COLUMN_COUNT, SOURCE_SYSTEM
        FROM META.DV_PROFILING_RUN WHERE RUN_ID = '{run_id}'
    """).to_pandas().iloc[0]
    _src_system_badge(str(info_quick.get("SOURCE_SYSTEM") or ""))
    st.caption(
        f"Method: **{info_quick['PROFILING_METHOD']}** &nbsp;|&nbsp; "
        f"Rows: **{int(info_quick['ROW_COUNT'] or 0):,}** &nbsp;|&nbsp; "
        f"Columns: **{info_quick['COLUMN_COUNT']}**"
    )

    st.markdown("---")

    # ── Run summary metrics ───────────────────────────────────────────────────

    info = session.sql(f"""
        SELECT
            COALESCE(SOURCE_DATABASE,'—') AS DB,
            SOURCE_SYSTEM, SOURCE_SCHEMA, SOURCE_TABLE,
            ROW_COUNT, COLUMN_COUNT, PROFILING_METHOD,
            TO_CHAR(STARTED_AT,'YYYY-MM-DD HH24:MI:SS')  AS STARTED,
            TO_CHAR(COMPLETED_AT,'YYYY-MM-DD HH24:MI:SS') AS COMPLETED,
            PROFILED_BY
        FROM META.DV_PROFILING_RUN WHERE RUN_ID = '{run_id}'
    """).to_pandas().iloc[0]

    def _metric_card(label, value):
        st.markdown(
            f"<div style='margin-bottom:8px'>"
            f"<span style='font-size:12px;color:#6b7280;font-weight:500'>{label}</span><br>"
            f"<span style='font-size:20px;font-weight:700;color:#111827'>{value}</span>"
            f"</div>",
            unsafe_allow_html=True
        )


    # ── Fetch metadata and profiling data (shared across tabs) ──────────────

    safe_db_ai     = info["DB"].replace("'", "''")
    safe_schema_ai = info["SOURCE_SCHEMA"].replace("'", "''")
    safe_table_ai  = info["SOURCE_TABLE"].replace("'", "''")

    try:
        comments_df = session.sql(f"""
            SELECT COLUMN_NAME, COMMENT
            FROM "{info['DB']}".INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{safe_schema_ai}'
              AND TABLE_NAME   = '{safe_table_ai}'
            ORDER BY ORDINAL_POSITION
        """).to_pandas()
        table_comment_row = session.sql(f"""
            SELECT COMMENT
            FROM "{info['DB']}".INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{safe_schema_ai}'
              AND TABLE_NAME   = '{safe_table_ai}'
        """).to_pandas()
        table_comment = (
            table_comment_row.iloc[0]["COMMENT"]
            if not table_comment_row.empty and table_comment_row.iloc[0]["COMMENT"]
            else None
        )
    except Exception:
        comments_df   = None
        table_comment = None

    prof_cols = session.sql(f"""
        SELECT
            COLUMN_NAME, SOURCE_DATA_TYPE, INFERRED_DATA_TYPE, PATTERN_DETECTED,
            ROUND(UNIQUENESS_RATIO*100,1)  AS UNIQUE_PCT,
            ROUND(NULL_PERCENTAGE,1)        AS NULL_PCT,
            DISTINCT_COUNT,
            MIN_VALUE, MAX_VALUE, MIN_LENGTH, MAX_LENGTH,
            TOP_VALUES::VARCHAR             AS TOP_VALUES,
            CHANGE_FREQUENCY,
            IS_PK_CANDIDATE
        FROM META.DV_PROFILING_RESULTS
        WHERE RUN_ID = '{run_id}'
        ORDER BY ORDINAL_POSITION
    """).to_pandas()

    if comments_df is not None:
        commented = set(
            comments_df[comments_df["COMMENT"].notna() & (comments_df["COMMENT"] != "")]["COLUMN_NAME"].str.upper()
        )
    else:
        commented = set()

    cols_needing_def = prof_cols[~prof_cols["COLUMN_NAME"].str.upper().isin(commented)]
    total_cols       = len(prof_cols)
    metadata_sparse  = (len(commented) / total_cols < 0.5) if total_cols > 0 else True

    # ── Build AI prompt helper ────────────────────────────────────────────────

    def _build_ai_prompt(app_ctx: str = "", tbl_ctx: str = ""):
        col_lines = []
        for _, r in prof_cols.iterrows():
            try:
                tv = json.loads(str(r["TOP_VALUES"])) if r["TOP_VALUES"] else []
                tv_str = ", ".join(str(v)[:40] for v in tv[:3])
            except Exception:
                tv_str = ""
            col_lines.append(
                f"- {r['COLUMN_NAME']} (type: {r['SOURCE_DATA_TYPE']}, "
                f"unique%: {r['UNIQUE_PCT']}, null%: {r['NULL_PCT']}, "
                f"distinct: {r['DISTINCT_COUNT']}, pattern: {r['PATTERN_DETECTED']}, "
                f"top values: [{tv_str}])"
            )
        col_summary = "\n".join(col_lines)

        # All columns get definitions — including those with existing comments (which may need enrichment)
        all_col_lines_for_def = "\n".join(
            f"- {r['COLUMN_NAME']} ({r['SOURCE_DATA_TYPE']})"
            + (f"  [existing description: {comments_df[comments_df['COLUMN_NAME'].str.upper() == r['COLUMN_NAME'].upper()]['COMMENT'].values[0]}]"
               if comments_df is not None and r['COLUMN_NAME'].upper() in commented else "")
            for _, r in prof_cols.iterrows()
        )

        table_ctx = (
            f"Table: {info['DB']}.{info['SOURCE_SCHEMA']}.{info['SOURCE_TABLE']}\n"
            f"Source System: {info['SOURCE_SYSTEM']}\n"
            f"Rows: {int(info['ROW_COUNT'] or 0):,}   Columns: {total_cols}\n"
        )
        if table_comment:
            table_ctx += f"Existing table description: {table_comment}\n"

        modeler_context = ""
        if app_ctx.strip():
            modeler_context += f"\n## Modeler Input: Application Context\n{app_ctx.strip()}\n"
        if tbl_ctx.strip():
            modeler_context += f"\n## Modeler Input: Table / Entity Context\n{tbl_ctx.strip()}\n"

        return (
            "You are a senior data architect analysing a source table for Data Vault 2.0 modelling.\n\n"
            "RULES:\n"
            "1. Always make an intelligent guess about why this dataset exists and how it is used in the "
            "business, even if no modeler context is provided. Reason from table name, source system name, "
            "column names, patterns, and top values.\n"
            "2. Where modeler context IS provided, treat it as authoritative and use it to anchor "
            "every section of your response.\n"
            "3. For EVERY column you MUST output a fully enriched business definition — no exceptions, no skipping. "
            "This rule applies whether or not an [existing description] is provided:\n"
            "   • If an [existing description] is shown: use it as context about the column's general meaning, "
            "but the output definition MUST add domain-specific enrichment drawn from the application context, "
            "table context, and your reasoned understanding of the dataset. "
            "The output must be meaningfully richer than the vanilla text — not a copy or a minor paraphrase. "
            "(e.g. vanilla: 'unique record identifier' → enriched: 'Unique identifier for a Bloomberg "
            "interest rate curve record, serving as the primary reference key across treasury pricing and risk systems').\n"
            "   • If NO [existing description] is shown: derive the definition entirely from the column name, "
            "data type, dataset context, and your domain reasoning. "
            "Never output a blank, 'N/A', or placeholder — always produce a substantive one-sentence definition.\n"
            "   CRITICAL: Column definitions must describe BUSINESS MEANING only. "
            "Do NOT mention null percentage, uniqueness ratio, distinct count, data patterns, or any other "
            "profiling statistics in the definition text. Those are inputs to your reasoning — not outputs.\n"
            "   The output line MUST use the exact format: COLUMN_NAME: definition\n"
            "   Every column in the list must appear on its own output line — no column may be omitted.\n"
            "4. Do not invent column values or statistics not present in the profiling data.\n\n"
            + modeler_context +
            "\n## 1. Dataset Overview\n"
            "Write 3-5 sentences covering: what this table likely represents, what business process or "
            "application it supports, why this dataset would exist in this source system, and any notable "
            "characteristics (volume, reference vs transactional, update frequency patterns). "
            "If no modeler context is given, make your best inference from the table name, source system, "
            "column names, and data patterns — and state your reasoning.\n\n"
            "## 2. Column Definitions\n"
            "Provide a business definition for every column. For columns with an existing description, "
            "enrich or correct it using the application and table context so it reflects the specific "
            "business meaning in this domain — not just a generic data type description. "
            "Format each line exactly as: COLUMN_NAME: definition\n"
            f"{all_col_lines_for_def}\n\n"
            "## 3. Business Key Recommendation\n"
            "Identify the most likely business key (primary key) for this table. "
            "Do NOT rely solely on uniqueness ratios — many columns can be statistically unique. "
            "Reason from: column names (IDs, codes, names), data patterns (UUID, NUMERIC_CODE), "
            "null rates, the nature of the dataset, top values, and any modeler-provided context. "
            "For reference/lookup tables the natural descriptor is often the key; "
            "for transactional tables look for surrogate or natural ID columns. "
            "Respond with exactly this format on one line:\n"
            "RECOMMENDED_BK: COLUMN1 [, COLUMN2]\n"
            "Then explain your reasoning in 2-3 sentences.\n\n"
            "## 4. Data Category & Business Concepts\n"
            "Classify this dataset and identify the core business concepts it represents.\n\n"
            "Choose exactly ONE data category:\n"
            "- MASTER_DATA: long-lived entities that other data references (customers, products, accounts, "
            "counterparties, employees, instruments). Rows persist and are updated over time.\n"
            "- TRANSACTIONAL_DATA: records of events or activities (trades, orders, payments, claims, "
            "bookings). Rows are created and generally not updated.\n"
            "- REFERENCE_DATA: lookup/code tables, classifications, calendars, currency lists, "
            "exchange rates, static mappings. Usually small, rarely changes.\n\n"
            "Respond with EXACTLY these two lines first (machine-readable):\n"
            "DATA_CATEGORY: MASTER_DATA|TRANSACTIONAL_DATA|REFERENCE_DATA\n"
            "CORE_CONCEPTS: concept1, concept2, concept3\n\n"
            "Core concepts are the real-world business entities or processes this table describes "
            "(e.g. Customer, Trade, Interest Rate Curve, Product, Counterparty). List 2-5.\n\n"
            "Then explain your classification in 2-3 sentences: why you chose this category "
            "and how the core concepts relate to each other in the business context.\n\n"
            "## 5. Sensitive Data Classification\n"
            "For each column, classify its data sensitivity using the definitions below. "
            "Reason from the column name, data type, top values, profiling patterns, and dataset context — "
            "not just name matching.\n"
            "- PII: directly identifies a natural person (name, email, phone, address, national ID, "
            "date of birth, IP address, device ID)\n"
            "- PHI: health or medical data (patient ID, diagnosis, prescription, medical record, "
            "clinical information, insurance member ID)\n"
            "- PCI: payment card data (card number, CVV, expiry date, account number, IBAN, routing number)\n"
            "- SPI: sensitive personal information that is not PII/PHI/PCI (salary, bonus, criminal history, "
            "biometrics, religious or political beliefs, union membership, performance ratings)\n"
            "- None: no sensitivity concern\n\n"
            "Output EXACTLY one line per column in this format (no prose, no skipping):\n"
            "SENSITIVITY: COLUMN_NAME: PII|PHI|PCI|SPI|None\n\n"
            + (
            "## 6. Context Validation\n"
            "Compare the modeler-provided context against the actual profiling data "
            "(column names, data types, top values, row count, patterns).\n"
            "Identify genuine contradictions — for example: modeler says this is a customer master but "
            "columns are clearly transactional; modeler names a primary key column that has high null rate; "
            "modeler describes a domain that does not match the column vocabulary at all.\n"
            "Do NOT flag if context is merely incomplete, general, or unverifiable.\n"
            "Output EXACTLY these machine-readable lines (no prose before them):\n"
            "CONTEXT_FIT: OK  (if context is consistent with the data, or no context was provided)\n"
            "CONTEXT_FIT: MISMATCH  (if one or more genuine contradictions were found)\n"
            "If MISMATCH, follow immediately with one line per issue:\n"
            "MISMATCH: <concise description of the specific contradiction>\n"
            "List only real contradictions — be precise and factual.\n\n"
            if (app_ctx.strip() or tbl_ctx.strip()) else ""
            )
            + f"--- PROFILING DATA ---\n{table_ctx}\nColumns:\n{col_summary}"
        )

    # ── AI model selector ─────────────────────────────────────────────────────

    AI_MODELS = {
        "Claude Haiku 4.5  (fastest)":       "claude-haiku-4-5",
        "Claude Sonnet 4.6  (balanced)":     "claude-sonnet-4-6",
        "Claude Opus 4.6  (best quality)":   "claude-opus-4-6",
    }
    _default_model_idx = 0  # sonnet as default
    sel_model_label = st.selectbox(
        "AI Model for Analysis",
        list(AI_MODELS.keys()),
        index=st.session_state.get("ai_model_idx", _default_model_idx),
        key="ai_model_sel",
        help="Choose the Cortex model for AI Analysis. Sonnet is recommended for speed; Opus for deeper reasoning."
    )
    st.session_state["ai_model_idx"] = list(AI_MODELS.keys()).index(sel_model_label)
    ai_model = AI_MODELS[sel_model_label]

    # ── AI analysis state (not auto-run — modeler must trigger explicitly) ──────

    cached_ai_text = st.session_state.get("ai_analysis_text") or ""
    cached_ai_run  = st.session_state.get("ai_analysis_run")
    ai_fresh       = (cached_ai_run == run_id and bool(cached_ai_text))

    # ── Helper: parse column definitions from AI Section 2 ───────────────────

    def _parse_ai_col_defs(ai_text: str) -> dict:
        """Return {COLUMN_NAME_UPPER: definition} from Section 2 of AI output."""
        m = re.search(
            r"## 2\.\s*Column Definitions.*?\n(.*?)(?=\n## \d+\.|\Z)",
            ai_text, re.DOTALL | re.IGNORECASE)
        block = m.group(1) if m else ai_text
        result = {}
        current_col = None
        current_def = []
        for line in block.splitlines():
            stripped = line.strip()
            # Match optional leading bullet/dash/asterisk then COLUMN_NAME: definition
            lm = re.match(
                r"^[-•*]?\s*\*{0,2}([A-Z][A-Z0-9_]*)\*{0,2}\s*[:\-–]\s*(.+)",
                stripped, re.IGNORECASE)
            if lm:
                # Save any previous col accumulated
                if current_col and current_def:
                    result[current_col] = " ".join(current_def).strip().rstrip(".")
                current_col = lm.group(1).strip().upper()
                current_def = [lm.group(2).strip()]
            elif current_col and stripped and not stripped.startswith("##"):
                # Continuation line of a multi-line definition
                current_def.append(stripped)
            else:
                # Blank line or section header — flush current
                if current_col and current_def:
                    result[current_col] = " ".join(current_def).strip().rstrip(".")
                current_col = None
                current_def = []
        # Flush last accumulated col
        if current_col and current_def:
            result[current_col] = " ".join(current_def).strip().rstrip(".")
        return result

    # ── Load previously-saved (approved) definitions from internal DB table ─────

    _saved_col_defs_db  = {}    # {COLUMN_NAME_UPPER: definition}
    _saved_tbl_desc_db  = ""
    _saved_sensitivity_db = {}  # {COLUMN_NAME_UPPER: is_sensitive}
    try:
        _db_def_rows = session.sql(f"""
            SELECT COLUMN_NAME, DEFINITION, IS_SENSITIVE, TABLE_DESCRIPTION
            FROM META.DV_COLUMN_DEFINITIONS
            WHERE SOURCE_SCHEMA = '{safe_schema_ai}'
              AND SOURCE_TABLE  = '{safe_table_ai}'
        """).collect()
        for _dr in _db_def_rows:
            _col_u = _dr["COLUMN_NAME"].upper()
            if _dr["DEFINITION"]:
                _saved_col_defs_db[_col_u] = _dr["DEFINITION"]
            if _dr["IS_SENSITIVE"] and _dr["IS_SENSITIVE"] != "None":
                _saved_sensitivity_db[_col_u] = _dr["IS_SENSITIVE"]
            if _dr["TABLE_DESCRIPTION"] and not _saved_tbl_desc_db:
                _saved_tbl_desc_db = _dr["TABLE_DESCRIPTION"]
    except Exception:
        pass
    _has_db_saved_defs = bool(_saved_col_defs_db)

    # ── Parse AI structured outputs from cached text ─────────────────────────

    ai_recommended_bk  = None
    ai_data_category   = None
    ai_core_concepts   = None
    ai_context_fit     = None   # "OK" | "MISMATCH"
    ai_mismatches      = []     # list of mismatch description strings
    ai_sensitivity_map = {}     # {COLUMN_NAME_UPPER: "PII"|"PHI"|"PCI"|"SPI"|"None"}

    if ai_fresh and cached_ai_text:
        bk_match  = re.search(r"RECOMMENDED_BK\s*:\s*(.+?)(?:\n|$)", cached_ai_text, re.IGNORECASE)
        dc_match  = re.search(r"DATA_CATEGORY\s*:\s*(.+?)(?:\n|$)", cached_ai_text, re.IGNORECASE)
        cc_match  = re.search(r"CORE_CONCEPTS\s*:\s*(.+?)(?:\n|$)", cached_ai_text, re.IGNORECASE)
        cf_match  = re.search(r"CONTEXT_FIT\s*:\s*(.+?)(?:\n|$)", cached_ai_text, re.IGNORECASE)
        if bk_match: ai_recommended_bk = bk_match.group(1).strip()
        if dc_match: ai_data_category  = dc_match.group(1).strip()
        if cc_match: ai_core_concepts  = cc_match.group(1).strip()
        if cf_match:
            ai_context_fit = cf_match.group(1).strip().upper()
            if "MISMATCH" in ai_context_fit:
                ai_mismatches = [
                    m.group(1).strip()
                    for m in re.finditer(r"MISMATCH\s*:\s*(.+?)(?:\n|$)", cached_ai_text, re.IGNORECASE)
                ]
        # Parse sensitivity classifications (SENSITIVITY: COLUMN_NAME: CATEGORY)
        _valid_sens = {"PII", "PHI", "PCI", "SPI", "NONE"}

        # Isolate Section 5 if present — avoids false positives from other sections
        _sens_section_m = re.search(
            r"## 5\.\s*Sensitive.*?\n(.*?)(?=\n## \d+\.|\Z)",
            cached_ai_text, re.DOTALL | re.IGNORECASE)
        _sens_text = _sens_section_m.group(1) if _sens_section_m else cached_ai_text

        # Primary: strict format  SENSITIVITY: COL_NAME: CATEGORY
        # Column name accepts any chars between the two colons (handles mixed case,
        # spaces, hyphens) — \w+ replaced with explicit category list for safety
        for _sm in re.finditer(
                r"SENSITIVITY\s*:\s*([^\n:]+?)\s*:\s*(PII|PHI|PCI|SPI|None|NONE)\b",
                _sens_text, re.IGNORECASE):
            _col_u = _sm.group(1).strip().upper()
            _cat   = _sm.group(2).strip().upper()
            if _cat in _valid_sens:
                ai_sensitivity_map[_col_u] = "None" if _cat == "NONE" else _cat

        # Fallback: if primary matched nothing, try loose  COL_NAME: CATEGORY
        # within the section, validated against the profiled column list
        if not ai_sensitivity_map and _sens_section_m:
            _known_cols = {c.upper() for c in prof_cols["COLUMN_NAME"].tolist()}
            for _sm in re.finditer(
                    r"([A-Z][A-Z0-9_]+)\s*:\s*(PII|PHI|PCI|SPI|None|NONE)\b",
                    _sens_text, re.IGNORECASE):
                _col_u = _sm.group(1).strip().upper()
                _cat   = _sm.group(2).strip().upper()
                if _col_u in _known_cols and _cat in _valid_sens:
                    ai_sensitivity_map[_col_u] = "None" if _cat == "NONE" else _cat

        st.session_state[f"ai_sensitivity_{run_id}"] = ai_sensitivity_map

        # ── Persist enriched vault notes keyed by source for Design Workbench ─
        _src_key_pr = f"{info['SOURCE_SYSTEM']}__{info['SOURCE_TABLE']}"
        _vault_notes_parts = []

        _overview_m = re.search(
            r"## 1\.\s*Dataset Overview\s*\n(.+?)(?=\n## |\Z)", cached_ai_text, re.DOTALL | re.IGNORECASE)
        if _overview_m:
            _vault_notes_parts.append(
                f"DATASET OVERVIEW (from AI profiling analysis):\n{_overview_m.group(1).strip()}")

        if ai_data_category:
            _vault_notes_parts.append(f"DATA CATEGORY: {ai_data_category}")
        if ai_core_concepts:
            _vault_notes_parts.append(f"CORE BUSINESS CONCEPTS: {ai_core_concepts}")
        if ai_recommended_bk:
            _vault_notes_parts.append(f"RECOMMENDED BUSINESS KEY: {ai_recommended_bk}")

        _col_def_m = re.search(
            r"## 2\.\s*Column Definitions\s*\n(.+?)(?=\n## |\Z)", cached_ai_text, re.DOTALL | re.IGNORECASE)
        if _col_def_m:
            _vault_notes_parts.append(
                f"ENRICHED COLUMN DEFINITIONS (from AI profiling analysis):\n{_col_def_m.group(1).strip()}")

        if _vault_notes_parts:
            st.session_state.setdefault("ai_vault_notes", {})[_src_key_pr] = \
                "\n\n".join(_vault_notes_parts)

        # ── Seed editable defs grid from AI text (only if not already edited) ─
        # AI-fresh definitions take precedence; DB-saved defs fill any gaps.
        _defs_key = f"ai_col_defs_{run_id}"
        if _defs_key not in st.session_state:
            _parsed = _parse_ai_col_defs(cached_ai_text)
            # Start with any previously-saved (approved) defs as the baseline
            _defs_init = dict(_saved_col_defs_db)
            # Build a quick lookup of profiling stats for fallback definitions
            _prof_lookup = {r["COLUMN_NAME"].upper(): r for _, r in prof_cols.iterrows()}
            for col in prof_cols["COLUMN_NAME"].tolist():
                col_up = col.upper()
                ai_def = _parsed.get(col_up, "").strip()
                if ai_def:
                    # Fresh AI definition overrides saved for this column
                    _defs_init[col_up] = ai_def
                elif col_up not in _defs_init:
                    # No AI def and no saved def — use vanilla Snowflake comment if available,
                    # otherwise synthesise a minimal definition from profiling stats
                    _vanilla = ""
                    if comments_df is not None:
                        _vc_rows = comments_df[
                            comments_df["COLUMN_NAME"].str.upper() == col_up
                        ]
                        if not _vc_rows.empty:
                            _vanilla = (_vc_rows.iloc[0]["COMMENT"] or "").strip()
                    if _vanilla:
                        _defs_init[col_up] = _vanilla
                    else:
                        _pr = _prof_lookup.get(col_up)
                        if _pr is not None:
                            _dtype = _pr["SOURCE_DATA_TYPE"] or "unknown type"
                            _pat   = _pr["PATTERN_DETECTED"] or ""
                            _pat_note = f" Detected pattern: {_pat}." if _pat and _pat.upper() not in ("NONE","") else ""
                            _defs_init[col_up] = (
                                f"{col.replace('_',' ').title()} ({_dtype}).{_pat_note}"
                            )
            st.session_state[_defs_key] = _defs_init

        _tbl_desc_key = f"ai_tbl_desc_{run_id}"
        if _tbl_desc_key not in st.session_state:
            # Seed from AI Dataset Overview, fall back to DB-saved table description
            _ov_seed = re.search(
                r"## 1\.\s*Dataset Overview\s*\n(.*?)(?=\n## \d+\.|\Z)",
                cached_ai_text, re.DOTALL | re.IGNORECASE)
            st.session_state[_tbl_desc_key] = (
                _ov_seed.group(1).strip() if _ov_seed else _saved_tbl_desc_db)

    # ── Tabs: Column Statistics | AI Analysis | PK Candidates ─────────────────

    tab1, tab2, tab3 = st.tabs(["Column Statistics", "AI Analysis", "PK Candidates"])

    # ── Tab 1: Column Statistics ──────────────────────────────────────────────

    with tab1:
        col_data = session.sql(f"""
            SELECT
                ORDINAL_POSITION              AS "#",
                COLUMN_NAME,
                SOURCE_DATA_TYPE              AS "Source Type",
                INFERRED_DATA_TYPE            AS "Inferred Type",
                PATTERN_DETECTED              AS "Pattern",
                ROUND(UNIQUENESS_RATIO*100,2) AS "Unique %",
                ROUND(NULL_PERCENTAGE,2)      AS "Null %",
                DISTINCT_COUNT                AS "Distinct",
                MIN_LENGTH                    AS "Min Len",
                MAX_LENGTH                    AS "Max Len",
                CHANGE_FREQUENCY              AS "Change Freq"
            FROM META.DV_PROFILING_RESULTS
            WHERE RUN_ID = '{run_id}'
            ORDER BY ORDINAL_POSITION
        """).to_pandas()

        # ── Sensitive data — from AI session, falling back to DB-saved values ──
        _sens_cache = st.session_state.get(f"ai_sensitivity_{run_id}", {})
        # Seed session state from DB-saved sensitivity if not already populated
        if not _sens_cache and _saved_sensitivity_db:
            _sens_cache = _saved_sensitivity_db
            st.session_state[f"ai_sensitivity_{run_id}"] = _sens_cache
        if _sens_cache:
            col_data["Sensitive"] = col_data["COLUMN_NAME"].apply(
                lambda c: _sens_cache.get(c.upper(), "None")
            )
        else:
            col_data["Sensitive"] = "—"

        _SENS_COLOUR = {
            "PII":  "background-color:#fee2e2;color:#991b1b;font-weight:600",
            "PHI":  "background-color:#fef3c7;color:#92400e;font-weight:600",
            "PCI":  "background-color:#ede9fe;color:#5b21b6;font-weight:600",
            "SPI":  "background-color:#fce7f3;color:#9d174d;font-weight:600",
            "None": "",
        }

        _FREQ_COLOUR = {
            "FAST":   "background-color:#fff3cd",
            "SLOW":   "background-color:#d4edda",
            "STATIC": "background-color:#cce5ff",
        }
        styled = (
            col_data.style
            .map(lambda v: _FREQ_COLOUR.get(v, ""), subset=["Change Freq"])
            .map(lambda v: _SENS_COLOUR.get(v, ""), subset=["Sensitive"])
        )
        st.dataframe(styled, use_container_width=True)

        with st.expander("Column Detail"):
            chosen = st.selectbox("Column", col_data["COLUMN_NAME"].tolist(), key="col_det")
            if chosen:
                d = session.sql(f"""
                    SELECT * FROM META.DV_PROFILING_RESULTS
                    WHERE RUN_ID = '{run_id}'
                      AND COLUMN_NAME = '{chosen.replace("'","''")}' """).to_pandas().iloc[0]
                dc1, dc2 = st.columns(2)
                with dc1:
                    st.write(f"**Min:** `{d.get('MIN_VALUE','—')}`")
                    st.write(f"**Max:** `{d.get('MAX_VALUE','—')}`")
                    st.write(f"**Avg length:** {d.get('AVG_LENGTH','—')}")
                with dc2:
                    try:
                        tv = d.get("TOP_VALUES")
                        vals = json.loads(str(tv)) if isinstance(tv, str) else (list(tv) if tv else [])
                        st.write("**Top Values:**")
                        for v in vals: st.write(f"  • `{v}`")
                    except Exception:
                        pass

        # ── Change frequency chart ────────────────────────────────────────────
        st.markdown("---")
        st.subheader("Change Frequency Distribution")

        # Check which method was used
        freq_method_row = session.sql(f"""
            SELECT PROFILING_METHOD FROM META.DV_PROFILING_RUN WHERE RUN_ID = '{run_id}'
        """).collect()
        snap_tables_exist = session.sql(f"""
            SELECT COUNT(*) AS C FROM {f'"{info["DB"]}"'}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = UPPER('{info["SOURCE_SCHEMA"].replace("'","''")}')
              AND TABLE_NAME LIKE '{info["SOURCE_TABLE"].upper()}%'
              AND TABLE_NAME RLIKE '.*(_HIST|_HISTORY|_SNAP|_SNAPSHOT|_ARCHIVE)$'
        """).collect()[0][0]

        if snap_tables_exist:
            st.caption(
                "Change frequency derived from **snapshot/history table comparison** using the confirmed primary key."
            )
        else:
            st.warning(
                "**Disclaimer:** Change frequency is based on **column name pattern matching only** — "
                "no historical data was available for this table. "
                "To get accurate classifications, create a companion `_HIST` or `_SNAP` table "
                "populated with point-in-time snapshots and re-run profiling."
            )

        freq_df = session.sql(f"""
            SELECT COALESCE(CHANGE_FREQUENCY,'UNKNOWN') AS FREQ, COUNT(*) AS CNT
            FROM META.DV_PROFILING_RESULTS WHERE RUN_ID='{run_id}'
            GROUP BY CHANGE_FREQUENCY ORDER BY CNT DESC
        """).to_pandas()
        if not freq_df.empty:
            freq_df.columns = ["Change Frequency","Column Count"]
            st.bar_chart(freq_df.set_index("Change Frequency"))

    # ── Tab 2: AI Analysis ────────────────────────────────────────────────────

    with tab2:
        st.subheader("AI Analysis")
        st.caption(
            f"Uses **{sel_model_label.split('(')[0].strip()}** (via Snowflake Cortex) to generate enriched "
            "table and column definitions grounded in profiling data and domain context. "
            "Click **Run AI Analysis** below to start."
        )

        # ── Modeler context inputs ────────────────────────────────────────────
        with st.expander("💡 Provide context to guide the AI (optional)", expanded=False):
            st.caption(
                "Share what you know about this dataset. The AI will use this to sharpen its "
                "interpretation, column definitions, and business key recommendation."
            )
            ctx_col1, ctx_col2 = st.columns(2)
            with ctx_col1:
                st.markdown("**Application Context**")
                st.caption("What system/domain does this data come from? What business process does it support?")
                ai_app_ctx = st.text_area(
                    "", height=130, key="ai_tab_app_ctx",
                    label_visibility="collapsed",
                    placeholder=(
                        "e.g. Bloomberg feed for Treasury Operations. "
                        "This table contains daily interest rate curves used for bond pricing and risk."
                    ))
            with ctx_col2:
                st.markdown("**Table / Entity Context**")
                st.caption("Anything specific: known primary key, deprecated columns, relationships, column meanings.")
                ai_tbl_ctx = st.text_area(
                    "", height=130, key="ai_tab_tbl_ctx",
                    label_visibility="collapsed",
                    placeholder=(
                        "e.g. RATE_ID is the primary key. TENOR is the maturity bucket (1M, 3M, 1Y…). "
                        "LEGACY_CD is deprecated. Links to BOND_POSITION via CURVE_ID."
                    ))

        # ── Action buttons: always visible ───────────────────────────────────────
        _defs_key     = f"ai_col_defs_{run_id}"
        _tbl_desc_key = f"ai_tbl_desc_{run_id}"
        _ai_btn_c1, _ai_btn_c2, _ai_btn_spacer = st.columns([1, 1, 2])
        _run_label_inline = "↺ Re-run AI Analysis" if ai_fresh else "▶ Run AI Analysis"
        _do_rerun_ai = _ai_btn_c1.button(_run_label_inline, key="ai_run_btn", type="primary")
        _do_save     = _ai_btn_c2.button("💾 Save Definitions", key="ai_save_btn",
                                         disabled=not ai_fresh)

        if _do_rerun_ai:
            with st.spinner(f"Analysing with {sel_model_label.split('(')[0].strip()}…"):
                try:
                    import json as _json
                    _prompt_text = _build_ai_prompt(
                        app_ctx=ai_app_ctx,
                        tbl_ctx=ai_tbl_ctx
                    )
                    # Serialize to JSON then pass via params= to avoid ANY
                    # string escaping corruption (the .replace("'","''") approach
                    # breaks JSON escape sequences inside the prompt text).
                    _messages_json = _json.dumps(
                        [{"role": "user", "content": _prompt_text}],
                        ensure_ascii=True
                    )
                    result_row = session.sql(
                        """SELECT SNOWFLAKE.CORTEX.AI_COMPLETE(
                               ?,
                               PARSE_JSON(?),
                               OBJECT_CONSTRUCT('max_tokens', 20000, 'temperature', 0)
                           )::VARCHAR AS ANALYSIS""",
                        params=[ai_model, _messages_json]
                    ).collect()
                    _ai_raw = result_row[0]["ANALYSIS"] if result_row else ""
                    st.session_state["ai_analysis_text"] = _ai_raw
                    st.session_state["ai_analysis_run"]  = run_id
                    st.session_state.pop(_defs_key, None)
                    st.session_state.pop(_tbl_desc_key, None)
                    st.experimental_rerun()
                except Exception as e:
                    st.error(f"Cortex call failed: {e}")

        if not ai_fresh:
            if _has_db_saved_defs:
                st.info(
                    "Showing previously-saved definitions for this table. "
                    "Click **▶ Run AI Analysis** to generate fresh suggestions."
                )
                # Pre-load saved defs into session state so the grid renders below
                _defs_key_ns = f"ai_col_defs_{run_id}"
                if _defs_key_ns not in st.session_state:
                    st.session_state[_defs_key_ns] = dict(_saved_col_defs_db)
                _tbl_desc_key_ns = f"ai_tbl_desc_{run_id}"
                if _tbl_desc_key_ns not in st.session_state and _saved_tbl_desc_db:
                    st.session_state[_tbl_desc_key_ns] = _saved_tbl_desc_db
            else:
                st.info(
                    "AI analysis has not been run for this table yet. "
                    "Optionally add application and table context above, then click **▶ Run AI Analysis**."
                )

        if ai_fresh and cached_ai_text or (not ai_fresh and _has_db_saved_defs):

            # ── Context mismatch banner (red) ──────────────────────────────────
            if ai_context_fit and "MISMATCH" in ai_context_fit and ai_mismatches:
                _mm_items = "".join(
                    f"<li style='margin:4px 0'>{_m}</li>" for _m in ai_mismatches
                )
                st.markdown(
                    f"<div style='background:#fff5f5;border-left:5px solid #e53e3e;"
                    f"padding:10px 16px;border-radius:4px;margin-bottom:16px'>"
                    f"<span style='font-size:13px;font-weight:700;color:#c53030'>"
                    f"⚠ Context Mismatch Detected</span>"
                    f"<ul style='margin:8px 0 0 0;padding-left:18px;color:#c53030;font-size:13px'>"
                    f"{_mm_items}"
                    f"</ul>"
                    f"</div>",
                    unsafe_allow_html=True
                )

            # ── Data Category summary card ─────────────────────────────────────
            if ai_data_category or ai_core_concepts:
                _DC_STYLE = {
                    "MASTER_DATA":       ("#1a56db", "#ebf5ff", "Master Data"),
                    "TRANSACTIONAL_DATA":("#057a55", "#f3faf7", "Transactional Data"),
                    "REFERENCE_DATA":    ("#c05500", "#fff8f0", "Reference Data"),
                }
                _dc_clean = (ai_data_category or "").upper().replace(" ", "_")
                _dc_col, _dc_bg, _dc_label = _DC_STYLE.get(
                    _dc_clean, ("#6b7280", "#f9fafb", ai_data_category or "Unknown"))
                _concepts_html = ""
                if ai_core_concepts:
                    _chips = "".join(
                        f"<span style='background:{_dc_col}22;color:{_dc_col};"
                        f"border:1px solid {_dc_col}44;border-radius:12px;"
                        f"padding:2px 10px;margin:2px 4px 2px 0;font-size:12px;"
                        f"display:inline-block'>{c.strip()}</span>"
                        for c in ai_core_concepts.split(",") if c.strip()
                    )
                    _concepts_html = (
                        f"<div style='margin-top:8px'>"
                        f"<span style='font-size:11px;color:#6b7280;font-weight:600;'"
                        f">CORE CONCEPTS &nbsp;</span>{_chips}</div>"
                    )
                st.markdown(
                    f"<div style='background:{_dc_bg};border-left:5px solid {_dc_col};"
                    f"padding:10px 16px;border-radius:4px;margin-bottom:16px'>"
                    f"<span style='font-size:11px;color:#6b7280;font-weight:600'>"
                    f"DATA CATEGORY &nbsp;</span>"
                    f"<span style='font-size:15px;font-weight:700;color:{_dc_col}'>"
                    f"{_dc_label}</span>"
                    f"{_concepts_html}"
                    f"</div>",
                    unsafe_allow_html=True
                )

            # ── Section 1: Dataset Overview + Table Description side-by-side ──
            _ov_m = re.search(
                r"## 1\.\s*Dataset Overview\s*\n(.*?)(?=\n## \d+\.|\Z)",
                cached_ai_text, re.DOTALL | re.IGNORECASE)
            _ov_text = _ov_m.group(1).strip() if _ov_m else ""

            _tbl_desc_key = f"ai_tbl_desc_{run_id}"
            _tbl_desc_val = st.session_state.get(_tbl_desc_key, "")

            st.markdown("**Table Description**")
            _td_left, _td_right = st.columns(2)
            with _td_left:
                st.caption("Vanilla (existing Snowflake comment)")
                st.text_area("", value=table_comment or "— no existing description —",
                             height=110, disabled=True,
                             key=f"tbl_vanilla_{run_id}",
                             label_visibility="collapsed")
            with _td_right:
                st.caption("Enriched (editable)")
                new_tbl_desc = st.text_area(
                    "", value=_tbl_desc_val,
                    height=110, key=f"tbl_desc_input_{run_id}",
                    label_visibility="collapsed",
                    placeholder="AI-enriched table description…")
            st.session_state[_tbl_desc_key] = new_tbl_desc

            if _ov_text:
                with st.expander("Dataset Overview", expanded=True):
                    st.markdown(_ov_text)

            # ── Core Concepts section ─────────────────────────────────────────
            if ai_core_concepts:
                st.markdown("**Core Business Concepts**")
                _cc_style = {
                    "MASTER_DATA":       ("#1a56db", "#ebf5ff"),
                    "TRANSACTIONAL_DATA":("#057a55", "#f3faf7"),
                    "REFERENCE_DATA":    ("#c05500", "#fff8f0"),
                }
                _cc_clean = (ai_data_category or "").upper().replace(" ", "_")
                _cc_col, _cc_bg = _cc_style.get(_cc_clean, ("#4b5563", "#f3f4f6"))
                _concept_chips = "".join(
                    f"<span style='background:{_cc_bg};color:{_cc_col};"
                    f"border:1px solid {_cc_col}44;border-radius:16px;"
                    f"padding:5px 14px;margin:4px 6px 4px 0;font-size:13px;"
                    f"font-weight:600;display:inline-block'>{c.strip()}</span>"
                    for c in ai_core_concepts.split(",") if c.strip()
                )
                st.markdown(
                    f"<div style='padding:10px 0 4px 0'>{_concept_chips}</div>",
                    unsafe_allow_html=True
                )

            # ── Section 3: Business Key reasoning ────────────────────────────
            _bk_m = re.search(
                r"## 3\.\s*Business Key Recommendation\s*\n(.*?)(?=\n## \d+\.|\Z)",
                cached_ai_text, re.DOTALL | re.IGNORECASE)
            if _bk_m:
                _bk_text = re.sub(r"RECOMMENDED_BK\s*:.*?(\n|$)", "",
                                  _bk_m.group(1)).strip()
                if _bk_text:
                    with st.expander("Business Key Reasoning", expanded=False):
                        st.markdown(_bk_text)

            # ── Section 4: Data Category reasoning only (no DV modelling) ────
            _dc4_m = re.search(
                r"## 4\.\s*Data Category.*?\n(.*?)(?=\n## \d+\.|\Z)",
                cached_ai_text, re.DOTALL | re.IGNORECASE)
            if _dc4_m:
                # Strip machine-readable lines and any DV vault/hub/satellite sentences
                _cat_raw = _dc4_m.group(1)
                _cat_raw = re.sub(r"DATA_CATEGORY\s*:.*?(\n|$)", "", _cat_raw)
                _cat_raw = re.sub(r"CORE_CONCEPTS\s*:.*?(\n|$)", "", _cat_raw)
                # Remove sentences mentioning Data Vault modelling
                _cat_raw = re.sub(
                    r"[^.!?]*\b(data vault|hub|satellite|link table|dv model)\b[^.!?]*[.!?]",
                    "", _cat_raw, flags=re.IGNORECASE)
                _cat_text = _cat_raw.strip()
                if _cat_text:
                    with st.expander("Data Category Reasoning", expanded=False):
                        st.markdown(_cat_text)

            st.markdown("---")

            if _do_save:
                _final_defs = st.session_state.get(_defs_key, {})
                _final_tbl_desc = st.session_state.get(_tbl_desc_key, "")
                _final_sensitivity = st.session_state.get(f"ai_sensitivity_{run_id}", {})
                _defs_to_save = {c: d for c, d in _final_defs.items() if d.strip()}

                # ── 1. Save to META.DV_COLUMN_DEFINITIONS (primary, always works) ──
                db_saved, db_errors = 0, []
                if _defs_to_save:
                    try:
                        _merge_vals = []
                        for _col, _def in _defs_to_save.items():
                            _sens = _final_sensitivity.get(_col.upper(), "None")
                            _tbl_d = _final_tbl_desc.strip().replace("'", "''")
                            _merge_vals.append(
                                f"('{safe_schema_ai}','{safe_table_ai}',"
                                f"'{_col.replace(chr(39),chr(39)*2)}',"
                                f"'{_def.rstrip('.').replace(chr(39),chr(39)*2)}',"
                                f"'{_sens}',"
                                f"'{_tbl_d}')"
                            )
                        if _merge_vals:
                            session.sql(f"""
                                MERGE INTO META.DV_COLUMN_DEFINITIONS AS tgt
                                USING (
                                    SELECT col_vals.*
                                    FROM (VALUES {','.join(_merge_vals)}) AS col_vals(
                                        SOURCE_SCHEMA, SOURCE_TABLE, COLUMN_NAME,
                                        DEFINITION, IS_SENSITIVE, TABLE_DESCRIPTION)
                                ) AS src
                                ON  tgt.SOURCE_SCHEMA = src.SOURCE_SCHEMA
                                AND tgt.SOURCE_TABLE  = src.SOURCE_TABLE
                                AND tgt.COLUMN_NAME   = src.COLUMN_NAME
                                WHEN MATCHED THEN UPDATE SET
                                    tgt.DEFINITION        = src.DEFINITION,
                                    tgt.IS_SENSITIVE      = src.IS_SENSITIVE,
                                    tgt.TABLE_DESCRIPTION = src.TABLE_DESCRIPTION,
                                    tgt.SAVED_AT          = CURRENT_TIMESTAMP(),
                                    tgt.SAVED_BY          = CURRENT_USER()
                                WHEN NOT MATCHED THEN INSERT (
                                    SOURCE_SCHEMA, SOURCE_TABLE, COLUMN_NAME,
                                    DEFINITION, IS_SENSITIVE, TABLE_DESCRIPTION)
                                VALUES (
                                    src.SOURCE_SCHEMA, src.SOURCE_TABLE, src.COLUMN_NAME,
                                    src.DEFINITION, src.IS_SENSITIVE, src.TABLE_DESCRIPTION)
                            """).collect()
                            db_saved = len(_merge_vals)
                    except Exception as ex:
                        db_errors.append(str(ex))

                # ── 2. Try to propagate to source object comments (best-effort) ──
                # This fails for datashares, marketplace, and read-only objects.
                src_alter_blocked = False
                src_alter_errors  = []
                if _final_tbl_desc.strip():
                    try:
                        _safe_tbl_desc = _final_tbl_desc.strip().replace("'", "''")
                        session.sql(f"""
                            ALTER TABLE "{info['DB']}"."{info['SOURCE_SCHEMA']}"."{info['SOURCE_TABLE']}"
                            SET COMMENT = '{_safe_tbl_desc}'
                        """).collect()
                    except Exception:
                        src_alter_blocked = True

                for col_name, defn in _defs_to_save.items():
                    safe_col = col_name.replace('"', '""')
                    safe_def = defn.rstrip(".").replace("'", "''")
                    try:
                        session.sql(f"""
                            ALTER TABLE "{info['DB']}"."{info['SOURCE_SCHEMA']}"."{info['SOURCE_TABLE']}"
                            ALTER COLUMN "{safe_col}" COMMENT '{safe_def}'
                        """).collect()
                    except Exception:
                        src_alter_blocked = True
                        break  # stop trying once we know it's read-only

                # ── 3. Update in-session vault notes for Raw Vault generation ──
                _src_key_save = f"{info['SOURCE_SYSTEM']}__{info['SOURCE_TABLE']}"
                _edited_col_def_lines = "\n".join(
                    f"{c}: {d}" for c, d in _final_defs.items() if d.strip())
                _existing_vault = st.session_state.get("ai_vault_notes", {}).get(_src_key_save, "")
                _updated_vault = re.sub(
                    r"ENRICHED COLUMN DEFINITIONS.*",
                    f"ENRICHED COLUMN DEFINITIONS (modeler-reviewed):\n{_edited_col_def_lines}",
                    _existing_vault, flags=re.DOTALL)
                if "ENRICHED COLUMN DEFINITIONS" not in _existing_vault:
                    _updated_vault = (_existing_vault.strip() + "\n\n"
                        f"ENRICHED COLUMN DEFINITIONS (modeler-reviewed):\n{_edited_col_def_lines}")
                if _final_tbl_desc.strip():
                    _updated_vault = (
                        f"TABLE DESCRIPTION: {_final_tbl_desc.strip()}\n\n" +
                        re.sub(r"TABLE DESCRIPTION:.*?\n\n", "", _updated_vault, flags=re.DOTALL))
                st.session_state.setdefault("ai_vault_notes", {})[_src_key_save] = _updated_vault.strip()

                # ── 4. Feedback ───────────────────────────────────────────────
                if db_errors:
                    st.error(f"Failed to save definitions internally: {'; '.join(db_errors)}")
                elif db_saved:
                    if src_alter_blocked:
                        st.success(
                            f"Definitions saved internally for {db_saved} column(s). "
                            "Source object is read-only (datashare / marketplace) — "
                            "definitions will be applied to the Raw Vault model automatically."
                        )
                    else:
                        st.success(
                            f"Definitions saved for {db_saved} column(s) — "
                            "stored internally and applied to source object comments."
                        )
                else:
                    st.info("No definitions to save — fill in the Enriched Definition column first.")

            # ── Column definitions: manual side-by-side layout ────────────
            st.markdown("**Column Definitions**")
            st.caption("Vanilla (read-only)  |  Enriched (editable)")

            _current_defs = st.session_state.get(_defs_key, {})

            _vanilla_map = {}
            if comments_df is not None:
                for _, cr in comments_df.iterrows():
                    if cr["COMMENT"]:
                        _vanilla_map[cr["COLUMN_NAME"].upper()] = cr["COMMENT"]

            _h0, _h1, _h2 = st.columns([1.2, 2.4, 2.4])
            _h0.markdown("<span style='font-size:11px;font-weight:600;color:#6b7280'>COLUMN</span>",
                         unsafe_allow_html=True)
            _h1.markdown("<span style='font-size:11px;font-weight:600;color:#6b7280'>VANILLA</span>",
                         unsafe_allow_html=True)
            _h2.markdown("<span style='font-size:11px;font-weight:600;color:#6b7280'>ENRICHED</span>",
                         unsafe_allow_html=True)

            for _col in prof_cols["COLUMN_NAME"].tolist():
                _col_up = _col.upper()
                _c0, _c1, _c2 = st.columns([1.2, 2.4, 2.4])
                _c0.markdown(
                    f"<div style='padding-top:6px;font-size:12px;font-weight:600'>{_col}</div>",
                    unsafe_allow_html=True)
                _c1.text_area("", value=_vanilla_map.get(_col_up, ""),
                              height=68, disabled=True,
                              key=f"van_{run_id}_{_col_up}",
                              label_visibility="collapsed")
                _new_val = _c2.text_area(
                    "", value=_current_defs.get(_col_up, ""),
                    height=68,
                    key=f"enr_{run_id}_{_col_up}",
                    label_visibility="collapsed")
                _current_defs[_col_up] = _new_val or ""

            st.session_state[_defs_key] = _current_defs

        if not ai_fresh:
            st.info(
                "AI analysis has not been run for this table yet. "
                "Optionally add application and table context above, then click **▶ Run AI Analysis**."
            )

    # ── Tab 3: PK Candidates ──────────────────────────────────────────────────

    with tab3:
        st.subheader("Primary Key Candidates")

        # ── AI-recommended BK (shown at top when available) ───────────────────
        if ai_recommended_bk:
            st.markdown("#### AI-Recommended Business Key")
            st.success(f"**{ai_recommended_bk}**")
            # Show per-column stats for the recommended columns
            rec_cols = [c.strip().upper() for c in ai_recommended_bk.split(",")]
            in_list  = ", ".join(f"'{c}'" for c in rec_cols)
            rec_stats = session.sql(f"""
                SELECT
                    COLUMN_NAME                   AS "Column",
                    SOURCE_DATA_TYPE              AS "Type",
                    ROUND(UNIQUENESS_RATIO*100,2) AS "Unique %",
                    ROUND(NULL_PERCENTAGE,2)      AS "Null %",
                    DISTINCT_COUNT                AS "Distinct",
                    PATTERN_DETECTED              AS "Pattern"
                FROM META.DV_PROFILING_RESULTS
                WHERE RUN_ID = '{run_id}' AND UPPER(COLUMN_NAME) IN ({in_list})
                ORDER BY ORDINAL_POSITION
            """).to_pandas()
            if not rec_stats.empty:
                st.dataframe(rec_stats.reset_index(drop=True), use_container_width=True)
            st.caption("Recommended by Claude based on column semantics, patterns, and dataset characteristics — not purely uniqueness.")
            st.markdown("---")
        elif not ai_fresh:
            st.info("Run AI Analysis to get an intelligent business key recommendation.")

        pk_data = session.sql(f"""
            SELECT
                CANDIDATE_ID,
                COLUMN_NAMES::VARCHAR              AS "Columns",
                CANDIDATE_TYPE                     AS "Type",
                PK_SCORE                           AS "Score",
                SCORE_BREAKDOWN::VARCHAR           AS "Breakdown",
                IFF(PK_SCORE>=60,'✅ Strong',IFF(PK_SCORE>=40,'⚠️ Possible','❌ Weak')) AS "Strength",
                IFF(MODELER_SELECTED,'✔ Confirmed','') AS "Confirmed"
            FROM META.DV_PK_CANDIDATES
            WHERE RUN_ID = '{run_id}'
            ORDER BY
                CASE CANDIDATE_TYPE WHEN 'MODELER_SUGGESTED' THEN 0 ELSE 1 END,
                PK_SCORE DESC
        """).to_pandas()

        # ── Modeler-suggested PK ──────────────────────────────────────────────
        if not pk_data.empty:
            modeler_row = pk_data[pk_data["Type"] == "MODELER_SUGGESTED"]
            if not modeler_row.empty:
                mr = modeler_row.iloc[0]
                st.markdown("#### Modeler-Suggested Primary Key")
                try:
                    bd = json.loads(mr["Breakdown"]) if mr["Breakdown"] else {}
                except Exception:
                    bd = {}
                comp_uniq     = bd.get("composite_uniqueness_ratio")
                comp_null_pct = bd.get("composite_null_pct")
                distinct_cnt  = bd.get("distinct_count")
                mp1, mp2, mp3, mp4 = st.columns(4)
                mp1.metric("Columns",             mr["Columns"])
                mp2.metric("Composite Unique %",  f"{round(float(comp_uniq)*100,2)}%" if comp_uniq is not None else "—")
                mp3.metric("Composite Null %",    f"{round(float(comp_null_pct),2)}%"  if comp_null_pct is not None else "—")
                mp4.metric("Distinct Combinations", f"{int(distinct_cnt):,}"           if distinct_cnt is not None else "—")
                if comp_uniq is not None:
                    if float(comp_uniq) == 1.0 and (not comp_null_pct or float(comp_null_pct) == 0):
                        st.success("Perfect unique key — no duplicates, no nulls.")
                    elif float(comp_uniq) >= 0.95:
                        st.warning("Near-unique key (≥95%). Verify duplicates before approving.")
                    else:
                        st.error("This combination is NOT unique. Consider revising your PK selection.")

                suggested_cols = bd.get("columns", [])
                if suggested_cols:
                    in_list2 = ", ".join(f"'{c.upper()}'" for c in suggested_cols)
                    pk_col_stats = session.sql(f"""
                        SELECT COLUMN_NAME AS "Column", SOURCE_DATA_TYPE AS "Type",
                               ROUND(UNIQUENESS_RATIO*100,2) AS "Unique %",
                               ROUND(NULL_PERCENTAGE,2) AS "Null %",
                               DISTINCT_COUNT AS "Distinct", PATTERN_DETECTED AS "Pattern"
                        FROM META.DV_PROFILING_RESULTS
                        WHERE RUN_ID='{run_id}' AND UPPER(COLUMN_NAME) IN ({in_list2})
                        ORDER BY ORDINAL_POSITION
                    """).to_pandas()
                    if not pk_col_stats.empty:
                        st.dataframe(pk_col_stats.reset_index(drop=True), use_container_width=True)
                st.markdown("---")

            # ── Auto-detected candidates ──────────────────────────────────────
            auto_data = pk_data[pk_data["Type"] != "MODELER_SUGGESTED"]
            if not auto_data.empty:
                st.markdown("#### Statistically Detected Candidates")
                st.caption("Based on uniqueness ratios only — use AI recommendation above for semantic guidance.")
                st.dataframe(
                    auto_data[["Columns","Type","Score","Strength","Confirmed"]],
                    use_container_width=True
                )

            # ── Confirm business key ──────────────────────────────────────────
            st.markdown("---")
            st.markdown("**Confirm Business Key**")
            all_candidate_cols = pk_data["Columns"].tolist()
            # Prepend AI recommendation as first option if not already present
            if ai_recommended_bk and ai_recommended_bk not in all_candidate_cols:
                all_candidate_cols = [ai_recommended_bk] + all_candidate_cols
            selected_bk = st.selectbox("Select to confirm:", all_candidate_cols, key="bk_sel")

            if st.button("✔ Confirm as Business Key", key="bk_confirm_btn"):
                # If it's the AI recommendation (not yet in DB), insert it first
                if selected_bk not in pk_data["Columns"].tolist():
                    bk_cols_list = [c.strip().upper() for c in selected_bk.split(",")]
                    session.sql(
                        """INSERT INTO META.DV_PK_CANDIDATES
                               (RUN_ID, COLUMN_NAMES, CANDIDATE_TYPE, PK_SCORE,
                                MODELER_SELECTED, SELECTED_BY, SELECTED_DATE)
                           SELECT ?, PARSE_JSON(?), 'AI_RECOMMENDED', 90,
                                  TRUE, CURRENT_USER(), CURRENT_TIMESTAMP()""",
                        params=[run_id, json.dumps(bk_cols_list)]
                    ).collect()
                else:
                    cand_id = int(pk_data[pk_data["Columns"] == selected_bk]["CANDIDATE_ID"].values[0])
                    session.sql(f"""
                        UPDATE META.DV_PK_CANDIDATES SET MODELER_SELECTED=FALSE
                        WHERE RUN_ID='{run_id}'
                    """).collect()
                    session.sql(f"""
                        UPDATE META.DV_PK_CANDIDATES
                        SET MODELER_SELECTED=TRUE, SELECTED_BY=CURRENT_USER(), SELECTED_DATE=CURRENT_TIMESTAMP()
                        WHERE CANDIDATE_ID={cand_id}
                    """).collect()
                session.sql(
                    """INSERT INTO META.DV_AUDIT_LOG
                           (ACTION_TYPE,ENTITY_TYPE,ENTITY_ID,SOURCE_TABLE,SOURCE_SYSTEM,ACTION_DETAILS)
                       SELECT 'PK_CONFIRM','RUN',?,?,?,PARSE_JSON(?)""",
                    params=[run_id,
                            info["SOURCE_TABLE"],
                            info["SOURCE_SYSTEM"],
                            json.dumps({"run_id": run_id, "confirmed_bk": selected_bk})]
                ).collect()
                st.success(f"Business Key confirmed: **{selected_bk}**")
                st.experimental_rerun()
        else:
            st.warning("No PK candidates detected yet.")

        # ── Modeler PK override ───────────────────────────────────────────────
        st.markdown("---")
        st.markdown("**Modeler PK Override**")
        st.caption("Enter your own primary key. For composite keys use comma-separated column names.")
        pk_override = st.text_input(
            "Primary key column(s):", key="pk_override_input",
            placeholder="CUSTOMER_ID  or  POLICY_ID, LINE_NBR"
        )
        if st.button("💾 Save PK Override", key="pk_override_btn"):
            if pk_override.strip():
                cols_clean = ", ".join([c.strip().upper() for c in pk_override.split(",") if c.strip()])
                session.sql(
                    """INSERT INTO META.DV_PK_CANDIDATES
                           (RUN_ID, COLUMN_NAMES, CANDIDATE_TYPE, PK_SCORE,
                            MODELER_SELECTED, SELECTED_BY, SELECTED_DATE)
                       SELECT ?, PARSE_JSON(?), 'MODELER_OVERRIDE', 100,
                              TRUE, CURRENT_USER(), CURRENT_TIMESTAMP()""",
                    params=[run_id, '["' + '","'.join(
                        [c.strip().upper() for c in pk_override.split(",") if c.strip()]
                    ) + '"]']
                ).collect()
                session.sql(f"""
                    UPDATE META.DV_PK_CANDIDATES SET MODELER_SELECTED=FALSE
                    WHERE RUN_ID='{run_id}' AND CANDIDATE_TYPE != 'MODELER_OVERRIDE'
                """).collect()
                st.success(f"PK override saved: **{cols_clean}**")
                st.experimental_rerun()
            else:
                st.warning("Enter at least one column name.")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3: DESIGN WORKBENCH
# ══════════════════════════════════════════════════════════════════════════════

def _merge_vault_notes(src_key: str, modeler_notes: str = "") -> str:
    """
    Build the modeler_notes string passed to SP_GENERATE_DV_PROPOSAL.
    Prepends any cached AI profiling analysis (data category, core concepts,
    enriched column defs) so the Raw Vault SP has full context.
    Also pulls approved definitions from META.DV_COLUMN_DEFINITIONS so they
    persist across sessions (important for datashares / read-only sources).
    """
    ai_notes = st.session_state.get("ai_vault_notes", {}).get(src_key, "")
    parts = []
    if ai_notes:
        parts.append(ai_notes)

    # Pull saved (approved) definitions from DB — survives session restarts
    # src_key format: "SOURCE_SYSTEM__SOURCE_TABLE"
    try:
        _src_parts = src_key.split("__", 1)
        if len(_src_parts) == 2:
            _src_tbl = _src_parts[1].replace("'", "''")
            _db_rows = session.sql(f"""
                SELECT COLUMN_NAME, DEFINITION, TABLE_DESCRIPTION
                FROM META.DV_COLUMN_DEFINITIONS
                WHERE SOURCE_TABLE = '{_src_tbl}'
                ORDER BY COLUMN_NAME
            """).collect()
            if _db_rows:
                _db_def_lines = "\n".join(
                    f"{r['COLUMN_NAME']}: {r['DEFINITION']}"
                    for r in _db_rows if r["DEFINITION"])
                _db_tbl_desc = next(
                    (r["TABLE_DESCRIPTION"] for r in _db_rows if r["TABLE_DESCRIPTION"]), "")
                _db_block_parts = []
                if _db_tbl_desc:
                    _db_block_parts.append(f"TABLE DESCRIPTION: {_db_tbl_desc}")
                if _db_def_lines:
                    _db_block_parts.append(
                        f"APPROVED COLUMN DEFINITIONS (modeler-reviewed, use these verbatim "
                        f"for column_definition in the vault model):\n{_db_def_lines}")
                if _db_block_parts:
                    _db_block = "\n\n".join(_db_block_parts)
                    # Only add if not already in ai_notes (avoid duplicate)
                    if "APPROVED COLUMN DEFINITIONS" not in ai_notes:
                        parts.append(_db_block)
    except Exception:
        pass

    if modeler_notes and modeler_notes.strip():
        parts.append("ADDITIONAL MODELER INSTRUCTIONS:\n" + modeler_notes.strip())
    return "\n\n".join(parts) or None


# ── Confidence badge colours ──────────────────────────────────────────────────
CONF_COLOUR = {"HIGH": "🟢", "MEDIUM": "🔵", "LOW": "🔴", "INFERRED": "⚪"}
ENTITY_TYPE_COLOUR = {"HUB": "🔵", "LNK": "🟢", "SAT": "🟠", "MSAT": "🟣", "ESAT": "🔴"}

# Entity card theme: border colour, background colour, display label
ENTITY_CARD_STYLE = {
    "HUB":  ("#1a56db", "#ebf5ff", "Hub"),
    "LNK":  ("#057a55", "#f3faf7", "Link"),
    "SAT":  ("#c05500", "#fff8f0", "Satellite"),
    "MSAT": ("#7e3af2", "#f5f3ff", "Multi-Active Sat"),
    "ESAT": ("#c81e1e", "#fff5f5", "Effectivity Sat"),
}

def _ws_key(*parts):
    """Generate a unique session-state key from parts."""
    return "__wb_" + "_".join(str(p) for p in parts)

def _clear_wb_widget_state():
    """
    Remove all workbench widget keys (__wb_*) from Streamlit session state.
    Must be called before loading a new workspace so that stale delete-checkbox
    states from the previous workspace do not silently filter out columns when
    the user saves.  (Streamlit preserves widget state by key across reruns, so
    a del-checkbox that was True in workspace A would still be True for the same
    key position in workspace B unless explicitly cleared.)
    """
    stale = [k for k in st.session_state if k.startswith("__wb_")]
    for k in stale:
        del st.session_state[k]

def _get_source_columns(run_id: str) -> list:
    """Return [(col_name, inferred_dtype), ...] from profiling results for a run."""
    if not run_id:
        return []
    try:
        rows = session.sql(f"""
            SELECT COLUMN_NAME,
                   COALESCE(INFERRED_DATA_TYPE, SOURCE_DATA_TYPE, 'VARCHAR') AS DT
            FROM META.DV_PROFILING_RESULTS
            WHERE RUN_ID = '{run_id.replace("'","''")}'
            ORDER BY ORDINAL_POSITION
        """).collect()
        return [(r['COLUMN_NAME'], r['DT']) for r in rows]
    except Exception:
        return []

def _load_workspace(workspace_id: str) -> dict:
    rows = session.sql(f"""
        SELECT WORKSPACE_JSON::VARCHAR AS WJ
        FROM META.DV_DESIGN_WORKSPACE
        WHERE WORKSPACE_ID = '{workspace_id.replace("'","''")}'
    """).collect()
    if rows:
        try:
            return json.loads(rows[0]['WJ'])
        except Exception:
            return {}
    return {}

def _save_workspace(workspace_id: str, ws: dict):
    ws_json = json.dumps(ws, ensure_ascii=True)
    session.sql(
        """UPDATE META.DV_DESIGN_WORKSPACE
           SET WORKSPACE_JSON   = PARSE_JSON(?),
               LAST_MODIFIED    = CURRENT_TIMESTAMP(),
               LAST_MODIFIED_BY = CURRENT_USER()
           WHERE WORKSPACE_ID = ?""",
        params=[ws_json, workspace_id]
    ).collect()
    meta = ws.get("_meta", {})
    session.sql(
        """INSERT INTO META.DV_AUDIT_LOG
               (ACTION_TYPE, ENTITY_TYPE, ENTITY_ID, SOURCE_TABLE, SOURCE_SYSTEM, ACTION_DETAILS)
           SELECT 'SAVE', 'WORKSPACE', ?, ?, ?, PARSE_JSON(?)""",
        params=[workspace_id,
                meta.get("source_table", "?"),
                meta.get("source_system", "?"),
                json.dumps({"workspace_id": workspace_id})]
    ).collect()

def _approve_workspace(workspace_id: str, ws: dict):
    """Write approved entities to registry tables then mark workspace APPROVED."""
    meta = ws.get('_meta', {})
    src_table  = meta.get('source_table', '')
    src_system = meta.get('source_system', '')
    src_schema = meta.get('source_schema', '')

    for entity_list, etype in [('hubs','HUB'), ('links','LNK'), ('satellites','SAT')]:
        for ent in ws.get(entity_list, []):
            eid = ent.get('entity_id','').replace("'","''")
            if not eid:
                continue
            # Determine sat type override
            actual_type = ent.get('satellite_type', etype) if etype == 'SAT' else etype

            session.sql(f"""
                MERGE INTO META.DV_ENTITY AS tgt
                USING (SELECT
                    '{eid}' AS ENTITY_ID,
                    '{actual_type}' AS ENTITY_TYPE,
                    '{ent.get("logical_name","").replace("'","''")}' AS LOGICAL_NAME,
                    '{ent.get("domain","").replace("'","''")}' AS DOMAIN,
                    '{src_system.replace("'","''")}' AS SOURCE_SYSTEM,
                    '{ent.get("parent_entity_id","").replace("'","''")}' AS PARENT_ENTITY_ID,
                    'APPROVED' AS APPROVAL_STATUS,
                    CURRENT_USER() AS APPROVED_BY,
                    CURRENT_TIMESTAMP() AS APPROVED_DATE
                ) AS src ON tgt.ENTITY_ID = src.ENTITY_ID
                WHEN MATCHED THEN UPDATE SET
                    tgt.LOGICAL_NAME     = src.LOGICAL_NAME,
                    tgt.APPROVAL_STATUS  = src.APPROVAL_STATUS,
                    tgt.APPROVED_BY      = src.APPROVED_BY,
                    tgt.APPROVED_DATE    = src.APPROVED_DATE,
                    tgt.LAST_MODIFIED    = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT
                    (ENTITY_ID, ENTITY_TYPE, LOGICAL_NAME, DOMAIN, SOURCE_SYSTEM,
                     PARENT_ENTITY_ID, APPROVAL_STATUS, APPROVED_BY, APPROVED_DATE)
                VALUES
                    (src.ENTITY_ID, src.ENTITY_TYPE, src.LOGICAL_NAME, src.DOMAIN,
                     src.SOURCE_SYSTEM, src.PARENT_ENTITY_ID,
                     src.APPROVAL_STATUS, src.APPROVED_BY, src.APPROVED_DATE)
            """).collect()

            # Upsert columns
            for pos, col in enumerate(ent.get('columns', []), 1):
                cname = col.get('column_name','').replace("'","''")
                if not cname:
                    continue
                session.sql(f"""
                    MERGE INTO META.DV_ENTITY_COLUMN AS tgt
                    USING (SELECT
                        '{eid}' AS ENTITY_ID,
                        '{cname}' AS COLUMN_NAME,
                        '{col.get("logical_name","").replace("'","''")}' AS LOGICAL_NAME,
                        '{col.get("data_type","VARCHAR").replace("'","''")}' AS DATA_TYPE,
                        '{col.get("column_role","ATTR").replace("'","''")}' AS COLUMN_ROLE,
                        {pos} AS ORDINAL_POSITION,
                        '{col.get("column_definition","").replace("'","''")}' AS COLUMN_DEFINITION,
                        '{col.get("source_column","")[:1900].replace("'","''")}' AS SOURCE_COLUMN
                    ) AS src ON tgt.ENTITY_ID = src.ENTITY_ID AND tgt.COLUMN_NAME = src.COLUMN_NAME
                    WHEN MATCHED THEN UPDATE SET
                        tgt.LOGICAL_NAME = src.LOGICAL_NAME, tgt.DATA_TYPE = src.DATA_TYPE,
                        tgt.COLUMN_ROLE  = src.COLUMN_ROLE,  tgt.COLUMN_DEFINITION = src.COLUMN_DEFINITION
                    WHEN NOT MATCHED THEN INSERT
                        (ENTITY_ID, COLUMN_NAME, LOGICAL_NAME, DATA_TYPE, COLUMN_ROLE,
                         ORDINAL_POSITION, COLUMN_DEFINITION, SOURCE_COLUMN)
                    VALUES
                        (src.ENTITY_ID, src.COLUMN_NAME, src.LOGICAL_NAME, src.DATA_TYPE,
                         src.COLUMN_ROLE, src.ORDINAL_POSITION, src.COLUMN_DEFINITION, src.SOURCE_COLUMN)
                """).collect()

    # Write hash definitions
    for hd in ws.get('hash_definitions', []):
        eid  = hd.get('entity_id','')
        hkey = hd.get('hash_key_name','')
        if not eid or not hkey:
            continue
        src_cols_json = json.dumps(hd.get('source_columns', []))
        session.sql(
            """MERGE INTO META.DV_HASH_DEFINITION AS tgt
               USING (SELECT ? AS ENTITY_ID, ? AS HASH_KEY_NAME) AS src
                   ON tgt.ENTITY_ID = src.ENTITY_ID AND tgt.HASH_KEY_NAME = src.HASH_KEY_NAME
               WHEN MATCHED THEN UPDATE SET
                   tgt.SOURCE_COLUMNS = PARSE_JSON(?)
               WHEN NOT MATCHED THEN INSERT
                   (ENTITY_ID, HASH_KEY_NAME, HASH_TYPE, SOURCE_COLUMNS,
                    NULL_REPLACEMENT, DELIMITER, ALGORITHM, PREPROCESSING)
               VALUES (?, ?, ?, PARSE_JSON(?), ?, ?, ?, ?)""",
            params=[eid, hkey, src_cols_json,
                    eid, hkey,
                    hd.get("hash_type","BUSINESS_KEY"), src_cols_json,
                    hd.get("null_replacement","-1"),
                    hd.get("delimiter","||"),
                    hd.get("algorithm","SHA2_256"),
                    hd.get("preprocessing","UPPER(TRIM(value))")]
        ).collect()

    # Mark workspace APPROVED
    session.sql(f"""
        UPDATE META.DV_DESIGN_WORKSPACE
        SET STATUS = 'APPROVED', LAST_MODIFIED = CURRENT_TIMESTAMP(),
            LAST_MODIFIED_BY = CURRENT_USER()
        WHERE WORKSPACE_ID = '{workspace_id.replace("'","''")}'
    """).collect()

    session.sql(
        """INSERT INTO META.DV_AUDIT_LOG
               (ACTION_TYPE, ENTITY_TYPE, ENTITY_ID, SOURCE_TABLE, SOURCE_SYSTEM, ACTION_DETAILS)
           SELECT 'APPROVE', 'WORKSPACE', ?, ?, ?, PARSE_JSON(?)""",
        params=[workspace_id, src_table, src_system,
                json.dumps({"workspace_id": workspace_id})]
    ).collect()


def _render_entity_card(ws: dict, entity_list_key: str, idx: int,
                        source_cols: list = None) -> tuple:
    """
    Render one entity card with editable fields.
    Returns (updated_entity_dict, should_delete_bool).
    source_cols: list of (col_name, dtype) tuples from source table profiling.
    """
    ent    = ws[entity_list_key][idx]
    eid    = ent.get('entity_id', f'ENTITY_{idx}')
    etype  = ent.get('entity_type', entity_list_key[:3].upper())
    conf   = ent.get('confidence', 'MEDIUM')
    status = ent.get('entity_status', 'NEW')

    border_col, bg_col, type_label = ENTITY_CARD_STYLE.get(
        etype, ("#888", "#f8f8f8", etype))
    conf_icon = CONF_COLOUR.get(conf, "⚪")
    is_new_flag = "" if ent.get('is_new', True) else "  *(registry)*"

    exp_label = f"{type_label}: {eid}  {conf_icon}{is_new_flag}"
    # Hubs expanded by default; links and sats collapsed
    expanded = (etype == 'HUB')

    with st.expander(exp_label, expanded=expanded):

        # ── Header: entity type badge + name + delete button ──────────────────
        st.markdown(
            f"<div style='background:{bg_col};border-left:5px solid {border_col};"
            f"padding:6px 12px;border-radius:4px;margin-bottom:10px'>"
            f"<span style='font-weight:700;color:{border_col}'>{type_label}</span>"
            f"<span style='color:#6b7280;font-size:12px;margin-left:12px'>"
            f"confidence: {conf}</span>"
            f"</div>",
            unsafe_allow_html=True
        )

        name_col, del_col = st.columns([5, 1])
        new_eid = name_col.text_input(
            "Entity Name", value=eid,
            key=_ws_key(entity_list_key, idx, 'eid'))
        should_delete = del_col.button(
            "🗑 Delete", key=_ws_key(entity_list_key, idx, 'del_entity'),
            help="Remove this entity from the proposal")

        # ── Rationale as bullet points ─────────────────────────────────────────
        rationale = ent.get('rationale', '')
        if rationale:
            sentences = [s.strip().rstrip('.') for s in
                         re.split(r'(?<=[.!?])\s+|;\s*', rationale) if s.strip()]
            if sentences:
                bullets_html = "".join(f"<li>{s}.</li>" for s in sentences)
                st.markdown(
                    f"<div style='background:#f8fafc;border-left:3px solid #94a3b8;"
                    f"padding:8px 12px;margin:6px 0 10px 0;border-radius:3px'>"
                    f"<span style='font-size:12px;font-weight:600;color:#475569'>"
                    f"Why proposed:</span>"
                    f"<ul style='margin:4px 0 0 0;padding-left:18px;"
                    f"font-size:12px;color:#334155'>{bullets_html}</ul></div>",
                    unsafe_allow_html=True
                )

        # Context captions
        if ent.get('parent_entity_id'):
            st.caption(f"Parent hub: **{ent['parent_entity_id']}**")
        if ent.get('participating_hubs'):
            st.caption(f"Linked hubs: **{', '.join(ent.get('participating_hubs', []))}**")
        if ent.get('source_table'):
            st.caption(f"Source table: **{ent['source_table']}**")

        # ── Columns table ─────────────────────────────────────────────────────
        st.markdown("**Columns**")
        cols = ent.get('columns') or []   # `or []` guards against explicit None
        role_opts = ['HK', 'BK', 'FK_HK', 'HASHDIFF', 'META', 'ATTR', 'MAK']

        # Header row
        hdr = st.columns([2.5, 2, 1.2, 2.2, 0.5])
        for h, label in zip(hdr, ["Column Name", "Data Type", "Role", "Source Column", "Del"]):
            h.markdown(
                f"<span style='font-size:11px;font-weight:600;color:#6b7280'>{label}</span>",
                unsafe_allow_html=True)

        new_cols = []
        for ci, col in enumerate(cols):
            cc = st.columns([2.5, 2, 1.2, 2.2, 0.5])
            col_name = cc[0].text_input(
                "", value=col.get('column_name', ''),
                label_visibility="collapsed",
                key=_ws_key(entity_list_key, idx, 'col', ci, 'name'))
            col_type = cc[1].text_input(
                "", value=col.get('data_type', 'VARCHAR'),
                label_visibility="collapsed",
                key=_ws_key(entity_list_key, idx, 'col', ci, 'type'))
            role_val = col.get('column_role', 'ATTR')
            role_idx = role_opts.index(role_val) if role_val in role_opts else len(role_opts) - 1
            col_role = cc[2].selectbox(
                "", role_opts, index=role_idx,
                label_visibility="collapsed",
                key=_ws_key(entity_list_key, idx, 'col', ci, 'role'))
            # source_column — fall back to column_definition for older workspaces
            src_col_val = col.get('source_column') or col.get('column_definition', '') or ''
            col_src = cc[3].text_input(
                "", value=src_col_val,
                label_visibility="collapsed",
                key=_ws_key(entity_list_key, idx, 'col', ci, 'src'))
            del_col_flag = cc[4].checkbox(
                "", value=False,
                label_visibility="collapsed",
                key=_ws_key(entity_list_key, idx, 'col', ci, 'del'))
            if not del_col_flag:
                new_cols.append({**col,
                    'column_name':      col_name,
                    'data_type':        col_type,
                    'column_role':      col_role,
                    'source_column':    col_src,
                    'column_definition': col_src})  # keep both fields in sync

        # ── Add column ─────────────────────────────────────────────────────────
        st.markdown("")
        src_col_names = [c[0] for c in (source_cols or [])]
        if src_col_names:
            adc1, adc2 = st.columns([4, 1])
            col_options = ['— select source column —'] + src_col_names
            picked = adc1.selectbox(
                "", col_options, label_visibility="collapsed",
                key=_ws_key(entity_list_key, idx, 'add_pick'))
            if adc2.button("＋ Add", key=_ws_key(entity_list_key, idx, 'add_btn')):
                if picked != '— select source column —':
                    dtype = next((c[1] for c in source_cols if c[0] == picked), 'VARCHAR')
                    new_cols.append({
                        'column_name': picked, 'data_type': dtype,
                        'column_role': 'ATTR', 'source_column': picked,
                        'column_definition': picked})
        else:
            if st.button("＋ Add Column", key=_ws_key(entity_list_key, idx, 'add_col')):
                new_cols.append({
                    'column_name': 'NEW_COLUMN', 'data_type': 'VARCHAR',
                    'column_role': 'ATTR', 'source_column': '',
                    'column_definition': ''})

        updated_status = status
        if new_eid != eid or new_cols != cols:
            updated_status = 'MODIFIED' if status == 'EXISTING' else status

        updated_ent = {**ent,
            'entity_id':      new_eid,
            'logical_name':   ent.get('logical_name', ''),   # pass-through
            'domain':         ent.get('domain', ''),          # pass-through
            'columns':        new_cols,
            'entity_status':  updated_status}
        return updated_ent, should_delete


def _generate_dot(ws: dict) -> str:
    """Generate a Graphviz DOT diagram for the workspace entities."""
    # Colours per entity type
    FILL = {'HUB': '#cce5ff', 'LNK': '#d4edda', 'SAT': '#ffe8cc',
            'MSAT': '#ffe8cc', 'ESAT': '#ffe8cc'}
    BORDER = {'HUB': '#0066cc', 'LNK': '#28a745', 'SAT': '#fd7e14',
              'MSAT': '#fd7e14', 'ESAT': '#fd7e14'}

    lines = [
        'digraph DV {',
        '    rankdir=LR;',
        '    node [shape=box fontname="Arial" fontsize=11 style=filled];',
        '    edge [arrowhead=open color="#555555"];',
    ]

    def safe_id(eid):
        return '"' + eid.replace('"', '') + '"'

    def node_label(ent, etype):
        eid = ent.get('entity_id', '')
        key_cols = [c['column_name'] for c in ent.get('columns', [])
                    if c.get('column_role') in ('HK', 'FK_HK', 'BK', 'HASHDIFF')]
        col_lines = '\\n'.join(key_cols[:4])
        if col_lines:
            return f'{eid}\\n───────\\n{col_lines}'
        return eid

    all_entities = (
        [(e, 'HUB') for e in ws.get('hubs', [])] +
        [(e, 'LNK') for e in ws.get('links', [])] +
        [(e, 'SAT') for e in ws.get('satellites', [])]
    )

    for ent, etype in all_entities:
        sat_t = ent.get('satellite_type', etype)
        etype_actual = sat_t if etype == 'SAT' else etype
        fill   = FILL.get(etype_actual, '#ffe8cc')
        border = BORDER.get(etype_actual, '#fd7e14')
        label  = node_label(ent, etype_actual)
        nid    = safe_id(ent.get('entity_id', ''))
        lines.append(f'    {nid} [label="{label}" fillcolor="{fill}" color="{border}"];')

    # Satellite → parent hub
    for ent in ws.get('satellites', []):
        parent = ent.get('parent_entity_id', '')
        if parent:
            lines.append(f'    {safe_id(parent)} -> {safe_id(ent.get("entity_id",""))};')

    # Hub → link
    for ent in ws.get('links', []):
        for hub_id in ent.get('participating_hubs', []):
            lines.append(f'    {safe_id(hub_id)} -> {safe_id(ent.get("entity_id",""))};')

    lines.append('}')
    return '\n'.join(lines)


def page_design_workbench():
    # ── Session state init ────────────────────────────────────────────────────
    for k, v in {
        'wb_workspace_id':   None,
        'wb_workspace':      None,
        'wb_source_key':     None,
        'wb_flow':           None,    # 'modeled' | 'profiled'
        'wb_profiled_meta':  {},
        'wb_source_cols':    [],      # [(col_name, dtype), ...] for add-column dropdown
        'wb_ai_model':       'claude-opus-4-6',
        'wb_ai_model_idx':   2,       # index into DV_DESIGN_MODELS (opus = index 2)
    }.items():
        if k not in st.session_state:
            st.session_state[k] = v

    st.title("Design Workbench")
    # Source system badge shown once a workspace is loaded (set below)
    _wb_sys_placeholder = st.empty()

    # ── Load source lists ─────────────────────────────────────────────────────
    sources_df = session.sql("""
        WITH LATEST_SYS AS (
            -- Latest source system per schema+table from profiling
            SELECT SOURCE_SCHEMA, SOURCE_TABLE, SOURCE_SYSTEM
            FROM META.DV_PROFILING_RUN
            WHERE STATUS = 'COMPLETED'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY SOURCE_SCHEMA, SOURCE_TABLE
                ORDER BY COMPLETED_AT DESC
            ) = 1
        )
        SELECT
            w.WORKSPACE_ID,
            w.SOURCE_TABLE,
            w.SOURCE_SYSTEM,
            w.SOURCE_SCHEMA,
            COALESCE(r.SOURCE_DATABASE, '?')              AS SOURCE_DATABASE,
            w.STATUS                                       AS WS_STATUS,
            w.AI_CONFIDENCE,
            w.INPUT_SCENARIO,
            TO_CHAR(w.LAST_MODIFIED,'YYYY-MM-DD HH24:MI') AS LAST_MOD,
            r.RUN_ID                                       AS LATEST_RUN_ID
        FROM META.DV_DESIGN_WORKSPACE w
        JOIN LATEST_SYS ls
          ON ls.SOURCE_TABLE  = w.SOURCE_TABLE
         AND ls.SOURCE_SCHEMA = w.SOURCE_SCHEMA
         AND ls.SOURCE_SYSTEM = w.SOURCE_SYSTEM
        LEFT JOIN META.DV_PROFILING_RUN r
               ON r.SOURCE_TABLE  = w.SOURCE_TABLE
              AND r.SOURCE_SYSTEM = w.SOURCE_SYSTEM
              AND r.STATUS        = 'COMPLETED'
        WHERE w.STATUS != 'SUPERSEDED'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY w.SOURCE_TABLE, w.SOURCE_SYSTEM
            ORDER BY w.LAST_MODIFIED DESC
        ) = 1
        ORDER BY w.LAST_MODIFIED DESC
    """).to_pandas()

    profiled_df = session.sql("""
        WITH LATEST_SYS AS (
            -- Latest source system per schema+table from profiling
            SELECT SOURCE_SCHEMA, SOURCE_TABLE, SOURCE_SYSTEM,
                   COALESCE(SOURCE_DATABASE, '?') AS SRC_DB,
                   TO_CHAR(MAX(COMPLETED_AT), 'YYYY-MM-DD') AS LAST_PROFILED
            FROM META.DV_PROFILING_RUN
            WHERE STATUS = 'COMPLETED'
            GROUP BY SOURCE_SCHEMA, SOURCE_TABLE, SOURCE_DATABASE, SOURCE_SYSTEM
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY SOURCE_SCHEMA, SOURCE_TABLE
                ORDER BY MAX(COMPLETED_AT) DESC
            ) = 1
        ),
        ACTIVE_WS AS (
            -- Tables that already have a non-superseded workspace for their latest source system
            SELECT DISTINCT w.SOURCE_SCHEMA, w.SOURCE_TABLE, w.SOURCE_SYSTEM
            FROM META.DV_DESIGN_WORKSPACE w
            JOIN LATEST_SYS ls
              ON ls.SOURCE_TABLE  = w.SOURCE_TABLE
             AND ls.SOURCE_SCHEMA = w.SOURCE_SCHEMA
             AND ls.SOURCE_SYSTEM = w.SOURCE_SYSTEM
            WHERE w.STATUS != 'SUPERSEDED'
        )
        SELECT ls.SOURCE_TABLE, ls.SOURCE_SYSTEM, ls.SRC_DB, ls.SOURCE_SCHEMA, ls.LAST_PROFILED
        FROM LATEST_SYS ls
        LEFT JOIN ACTIVE_WS aw
               ON aw.SOURCE_TABLE  = ls.SOURCE_TABLE
              AND aw.SOURCE_SCHEMA = ls.SOURCE_SCHEMA
              AND aw.SOURCE_SYSTEM = ls.SOURCE_SYSTEM
        WHERE aw.SOURCE_TABLE IS NULL
        ORDER BY ls.LAST_PROFILED DESC
    """).to_pandas()

    # ── AI model selector ─────────────────────────────────────────────────────
    DV_DESIGN_MODELS = {
        "Claude Haiku 4.5  (fastest)":       "claude-haiku-4-5",
        "Claude Sonnet 4.6  (balanced)":     "claude-sonnet-4-6",
        "Claude Opus 4.6  (best quality)":   "claude-opus-4-6",
        "Llama 3.1 405B  (open source)":     "llama3.1-405b",
        "OpenAI GPT-5.1":                    "openai-gpt-5.1",
    }
    _wb_model_label = st.radio(
        "AI Model for Vault Generation",
        list(DV_DESIGN_MODELS.keys()),
        index=st.session_state.get("wb_ai_model_idx", 2),
        horizontal=True,
        key="wb_ai_model_radio",
        help="Model used when generating or re-generating a Raw Vault proposal."
    )
    st.session_state["wb_ai_model_idx"] = list(DV_DESIGN_MODELS.keys()).index(_wb_model_label)
    st.session_state["wb_ai_model"]     = DV_DESIGN_MODELS[_wb_model_label]
    _wb_model_id = DV_DESIGN_MODELS[_wb_model_label]

    col_explorer, col_main = st.columns([1, 3])

    # ══ LEFT PANEL: SOURCE TABLES ═════════════════════════════════════════════
    with col_explorer:
        st.subheader("Source Tables")

        tab_mg, tab_pnm = st.tabs(["Model Generated", "Ready for Model"])

        # ── Tab 1: Model Generated ────────────────────────────────────────────
        with tab_mg:
            if sources_df.empty:
                st.info("No workspaces yet. Profile a table and generate an AI proposal.")
            else:
                db_opts = sorted(sources_df['SOURCE_DATABASE'].dropna().unique().tolist())
                sel_db = st.selectbox("Database", db_opts or ['?'], key="wb_mg_db")

                filt1 = sources_df[sources_df['SOURCE_DATABASE'] == sel_db]
                schema_opts = sorted(filt1['SOURCE_SCHEMA'].dropna().unique().tolist()) or ['?']
                sel_sch = st.selectbox("Schema", schema_opts, key="wb_mg_schema")

                filt2 = filt1[filt1['SOURCE_SCHEMA'] == sel_sch]
                table_opts = sorted(filt2['SOURCE_TABLE'].dropna().unique().tolist()) or ['?']
                sel_tbl = st.selectbox("Table", table_opts, key="wb_mg_table")

                mask = (filt2['SOURCE_TABLE'] == sel_tbl)
                if mask.any():
                    row = filt2[mask].iloc[0]
                    _src_system_badge(str(row.get('SOURCE_SYSTEM') or ''))
                    st.text_input("Status",
                                  value=row['WS_STATUS'] or '—',
                                  disabled=True, key="wb_mg_status")
                    st.text_input("Confidence",
                                  value=row['AI_CONFIDENCE'] or '—',
                                  disabled=True, key="wb_mg_conf")
                    st.text_input("Created",
                                  value=row['LAST_MOD'] or '—',
                                  disabled=True, key="wb_mg_created")
                    if st.button("Open Model →", use_container_width=True, key="wb_mg_open"):
                        run_id_for_cols = row.get('LATEST_RUN_ID')
                        _clear_wb_widget_state()
                        st.session_state.wb_workspace_id  = row['WORKSPACE_ID']
                        st.session_state.wb_workspace     = None
                        st.session_state.wb_source_key    = (
                            f"{row['SOURCE_SYSTEM']}__{row['SOURCE_TABLE']}")
                        st.session_state.wb_flow          = 'modeled'
                        st.session_state.wb_profiled_meta = {}
                        st.session_state.wb_source_cols   = (
                            _get_source_columns(run_id_for_cols)
                            if run_id_for_cols else [])
                        st.experimental_rerun()

        # ── Tab 2: Ready for Model ────────────────────────────────────────────
        with tab_pnm:
            if profiled_df.empty:
                st.info("All profiled tables already have workspaces.")
            else:
                pnm_db_opts = sorted(profiled_df['SRC_DB'].dropna().unique().tolist())
                sel_pnm_db = st.selectbox("Database", pnm_db_opts or ['?'], key="wb_pnm_db")

                pnm_filt1 = profiled_df[profiled_df['SRC_DB'] == sel_pnm_db]
                pnm_schema_opts = sorted(pnm_filt1['SOURCE_SCHEMA'].dropna().unique().tolist())
                sel_pnm_schema = st.selectbox("Schema", pnm_schema_opts or ['?'],
                                              key="wb_pnm_schema_sel",
                                              disabled=not pnm_schema_opts)

                pnm_filt2 = pnm_filt1[pnm_filt1['SOURCE_SCHEMA'] == sel_pnm_schema]
                pnm_table_opts = sorted(pnm_filt2['SOURCE_TABLE'].dropna().unique().tolist())
                sel_pnm_table = st.selectbox("Table", pnm_table_opts or ['?'],
                                             key="wb_pnm_table_sel",
                                             disabled=not pnm_table_opts)

                pnm_mask = (pnm_filt2['SOURCE_TABLE'] == sel_pnm_table)
                if pnm_mask.any():
                    pnm_row = pnm_filt2[pnm_mask].iloc[0]
                    st.caption(
                        f"{pnm_row['SOURCE_SYSTEM']}  ·  profiled {pnm_row['LAST_PROFILED']}")

                if st.button("🚀 Generate Raw Vault Model",
                             use_container_width=True, key="wb_pnm_gen_vault_btn",
                             disabled=not pnm_mask.any()):
                    pnm_row = pnm_filt2[pnm_filt2['SOURCE_TABLE'] == sel_pnm_table].iloc[0]
                    src_db_direct = pnm_row.get('SRC_DB', '?')
                    if src_db_direct == '?':
                        src_db_direct = None
                    _pnm_src_key = f"{pnm_row['SOURCE_SYSTEM']}__{pnm_row['SOURCE_TABLE']}"
                    with st.spinner("Calling Cortex AI… this may take 30–60 seconds"):
                        try:
                            ws_id_direct = session.call(
                                "META.SP_GENERATE_DV_PROPOSAL",
                                pnm_row['SOURCE_TABLE'],
                                pnm_row['SOURCE_SYSTEM'],
                                pnm_row.get('SOURCE_SCHEMA') or None,
                                src_db_direct, None,
                                _merge_vault_notes(_pnm_src_key),
                                _wb_model_id
                            )
                            lr_direct = session.sql(f"""
                                SELECT RUN_ID FROM META.DV_PROFILING_RUN
                                WHERE SOURCE_TABLE  = '{pnm_row['SOURCE_TABLE'].replace("'","''")}'
                                  AND SOURCE_SYSTEM = '{pnm_row['SOURCE_SYSTEM'].replace("'","''")}'
                                  AND STATUS = 'COMPLETED'
                                ORDER BY COMPLETED_AT DESC LIMIT 1
                            """).collect()
                            st.session_state.wb_source_cols   = (
                                _get_source_columns(lr_direct[0]['RUN_ID']) if lr_direct else [])
                            _clear_wb_widget_state()
                            st.session_state.wb_workspace_id  = ws_id_direct
                            st.session_state.wb_workspace     = None
                            st.session_state.wb_source_key    = (
                                f"{pnm_row['SOURCE_SYSTEM']}__{pnm_row['SOURCE_TABLE']}")
                            st.session_state.wb_flow          = 'modeled'
                            st.session_state.wb_profiled_meta = {}
                            st.experimental_rerun()
                        except Exception as e:
                            st.error(f"Generation failed: {e}")

    # ══ RIGHT PANEL: MAIN WORK AREA ════════════════════════════════════════════
    with col_main:
        flow = st.session_state.wb_flow

        if flow is None:
            st.info("Select a source from the left panel.")
            return

        # ── STATE A: Profiled, not yet modeled ────────────────────────────────
        if flow == 'profiled':
            pm = st.session_state.wb_profiled_meta
            st.caption(
                "This table is profiled but has no DV workspace yet. "
                "Fill in both context boxes below to help the AI generate a richer model.")
            st.markdown("---")

            ctx_c1, ctx_c2 = st.columns(2)
            with ctx_c1:
                st.markdown("**Application Context**")
                st.caption(
                    "Describe the business application this table belongs to: "
                    "its purpose, domain, and where this table fits within it.")
                app_ctx = st.text_area(
                    "", height=200, key="wb_pnm_app_ctx",
                    label_visibility="collapsed",
                    placeholder=(
                        "e.g. This table is part of the Core Banking System (CBS). "
                        "CBS manages all deposit and loan accounts. "
                        "This table is the master account record store used by "
                        "retail and commercial banking products."
                    ))
            with ctx_c2:
                st.markdown("**Table / Entity Context**")
                st.caption(
                    "Specific details about this table: primary key, deprecated columns, "
                    "relationships, any column meanings not obvious from the name.")
                ent_ctx = st.text_area(
                    "", height=200, key="wb_pnm_ent_ctx",
                    label_visibility="collapsed",
                    placeholder=(
                        "e.g. ACCT_ID is always the primary key. "
                        "STATUS_CD changes frequently (FAST change). "
                        "OPEN_DT is static after account creation. "
                        "LEGACY_REF column is deprecated — exclude it. "
                        "This table links to ACCT_TRANS via ACCT_ID."
                    ))

            if st.button("🤖 Generate AI Proposal",
                         use_container_width=True, key="wb_pnm_gen_btn"):
                _profiled_src_key = f"{pm['source_system']}__{pm['source_table']}"
                _user_notes = "\n\n".join(p for p in [app_ctx.strip(), ent_ctx.strip()] if p)
                with st.spinner("Calling Cortex AI… this may take 30–60 seconds"):
                    try:
                        src_db_param = pm.get('src_db')
                        if src_db_param == '?':
                            src_db_param = None
                        ws_id_new = session.call(
                            "META.SP_GENERATE_DV_PROPOSAL",
                            pm['source_table'], pm['source_system'],
                            pm.get('source_schema') or None,
                            src_db_param, None,
                            _merge_vault_notes(_profiled_src_key, _user_notes),
                            _wb_model_id
                        )
                        # Pre-cache source cols from profiling run
                        lr = session.sql(f"""
                            SELECT RUN_ID FROM META.DV_PROFILING_RUN
                            WHERE SOURCE_TABLE  = '{pm['source_table'].replace("'","''")}'
                              AND SOURCE_SYSTEM = '{pm['source_system'].replace("'","''")}'
                              AND STATUS = 'COMPLETED'
                            ORDER BY COMPLETED_AT DESC LIMIT 1
                        """).collect()
                        st.session_state.wb_source_cols = (
                            _get_source_columns(lr[0]['RUN_ID']) if lr else [])
                        _clear_wb_widget_state()
                        st.session_state.wb_workspace_id  = ws_id_new
                        st.session_state.wb_workspace     = None
                        st.session_state.wb_flow          = 'modeled'
                        st.experimental_rerun()
                    except Exception as e:
                        st.error(f"Generation failed: {e}")
            return

        # ── STATE B: Workspace loaded (modeled) ───────────────────────────────
        ws_id = st.session_state.wb_workspace_id
        if ws_id and st.session_state.wb_workspace is None:
            st.session_state.wb_workspace = _load_workspace(ws_id)
            # Populate source cols if not already cached
            if not st.session_state.wb_source_cols:
                run_id_ws = st.session_state.wb_workspace.get('_meta', {}).get('run_id')
                if not run_id_ws:
                    _src_tbl_tmp = (
                        st.session_state.wb_workspace.get('_meta', {}).get('source_table')
                        or (st.session_state.wb_source_key or '').split('__')[-1])
                    _src_sys_tmp = (
                        st.session_state.wb_workspace.get('_meta', {}).get('source_system')
                        or (st.session_state.wb_source_key or '').split('__')[0])
                    lr2 = session.sql(f"""
                        SELECT RUN_ID FROM META.DV_PROFILING_RUN
                        WHERE SOURCE_TABLE  = '{_src_tbl_tmp.replace("'","''")}'
                          AND SOURCE_SYSTEM = '{_src_sys_tmp.replace("'","''")}'
                          AND STATUS = 'COMPLETED'
                        ORDER BY COMPLETED_AT DESC LIMIT 1
                    """).collect()
                    run_id_ws = lr2[0]['RUN_ID'] if lr2 else None
                st.session_state.wb_source_cols = (
                    _get_source_columns(run_id_ws) if run_id_ws else [])

        ws = st.session_state.wb_workspace or {}
        source_cols = st.session_state.wb_source_cols

        meta    = ws.get('_meta', {})
        src_tbl = (meta.get('source_table')
                   or (st.session_state.wb_source_key or '').split('__')[-1])
        src_sys = (meta.get('source_system')
                   or (st.session_state.wb_source_key or '').split('__')[0])
        if src_sys:
            with _wb_sys_placeholder:
                _src_system_badge(src_sys)

        # ── Parse-error guard: workspace generated but AI JSON failed ─────────
        _has_entities = ws.get('hubs') or ws.get('links') or ws.get('satellites')
        _parse_failed = (not _has_entities and ws.get('warnings') and
                         any('could not be parsed' in (w or '').lower()
                             for w in ws.get('warnings', [])))
        if _parse_failed:
            # Auto-regenerate on first open — keyed per workspace so it fires
            # exactly once, then falls back to manual UI if it also fails.
            _auto_key = f"__wb_auto_regen_{ws_id}"
            if not st.session_state.get(_auto_key):
                st.session_state[_auto_key] = True
                _auto_src_key = f"{src_sys}__{src_tbl}"
                _auto_notes   = (meta.get('modeler_notes', '') or '').strip()
                with st.spinner(
                    "Previous AI response had a formatting error — "
                    "auto-recovering with a fresh generation…"):
                    try:
                        if ws_id:
                            session.sql(f"""
                                UPDATE META.DV_DESIGN_WORKSPACE SET STATUS='SUPERSEDED'
                                WHERE WORKSPACE_ID='{ws_id.replace("'","''")}'
                            """).collect()
                        _auto_id = session.call(
                            "META.SP_GENERATE_DV_PROPOSAL",
                            src_tbl, src_sys,
                            meta.get('source_schema'), None, None,
                            _merge_vault_notes(_auto_src_key, _auto_notes),
                            _wb_model_id
                        )
                        _clear_wb_widget_state()
                        st.session_state.wb_workspace_id = _auto_id
                        st.session_state.wb_workspace    = None
                        st.session_state.wb_source_cols  = []
                        st.experimental_rerun()
                    except Exception:
                        pass  # auto-regen failed — fall through to manual UI

            # Manual fallback (shown if auto-regen itself failed)
            parse_warn = next(
                (w for w in ws.get('warnings', []) if 'could not be parsed' in (w or '').lower()),
                '')
            st.error("AI response could not be parsed. Auto-recovery also failed.")
            if parse_warn:
                with st.expander("Technical detail"):
                    st.caption(parse_warn)
            raw_snippet = ws.get('_raw_ai_response', '')
            if raw_snippet:
                with st.expander("Show raw AI response (for diagnosis)"):
                    st.code(raw_snippet[:3000], language="text")
            existing_notes = meta.get('modeler_notes', '') or ''
            regen_notes_err = st.text_area(
                "Additional instructions (optional):", height=80,
                key="wb_err_regen_notes",
                placeholder="Leave blank to re-use the original instructions.")
            if st.button("🔄 Re-generate", use_container_width=True, key="wb_err_regen_btn"):
                _err_src_key = f"{src_sys}__{src_tbl}"
                _err_new  = regen_notes_err.strip()
                _err_old  = existing_notes.strip()
                if _err_new and _err_old:
                    _err_modeler = (
                        f"CHANGE REQUEST (OVERRIDE — APPLY EXACTLY AS STATED):\n{_err_new}"
                        f"\n\nORIGINAL MODELER NOTES (still apply unless contradicted above):\n{_err_old}"
                    )
                elif _err_new:
                    _err_modeler = _err_new
                else:
                    _err_modeler = _err_old
                _new_ws_id = None
                _regen_err = None
                with st.spinner("Calling Cortex AI… this may take 30–60 seconds"):
                    try:
                        if ws_id:
                            session.sql(f"""
                                UPDATE META.DV_DESIGN_WORKSPACE SET STATUS='SUPERSEDED'
                                WHERE WORKSPACE_ID='{ws_id.replace("'","''")}'
                            """).collect()
                        _new_ws_id = session.call(
                            "META.SP_GENERATE_DV_PROPOSAL",
                            src_tbl, src_sys,
                            meta.get('source_schema'), None, None,
                            _merge_vault_notes(_err_src_key, _err_modeler),
                            _wb_model_id
                        )
                    except Exception as e:
                        _regen_err = str(e)
                if _regen_err:
                    st.error(f"Re-generation failed: {_regen_err}")
                elif _new_ws_id:
                    _clear_wb_widget_state()
                    st.session_state.wb_workspace_id = _new_ws_id
                    st.session_state.wb_workspace    = None
                    st.session_state.wb_source_cols  = []
                    st.experimental_rerun()
            return

        # ── Tabs: DV Model + Diagram (no Metadata tab — use Profiling Review) ─
        tab_model, tab_diagram = st.tabs(["✏️ DV Model", "📊 Diagram"])

        # ── TAB 1: DV Model ───────────────────────────────────────────────────
        with tab_model:
            if not ws or not _has_entities:
                st.info("No workspace loaded or no entities generated yet. "
                        "Select a source from the left panel or generate an AI proposal.")
            else:
                total = (len(ws.get('hubs', [])) +
                         len(ws.get('links', [])) +
                         len(ws.get('satellites', [])))
                st.caption(
                    f"{total} entities  ·  "
                    f"Hubs → Links → Satellites  ·  "
                    "Edit inline, then Save or Approve All.")

                new_ws = dict(ws)

                # ── Hubs ──────────────────────────────────────────────────────
                if ws.get('hubs'):
                    st.markdown("### Hubs")
                    new_hubs = []
                    for i, ent in enumerate(ws['hubs']):
                        ws['hubs'][i] = {**ent, 'entity_type': 'HUB'}
                        updated, do_del = _render_entity_card(
                            ws, 'hubs', i, source_cols)
                        if not do_del:
                            new_hubs.append(updated)
                    new_ws['hubs'] = new_hubs

                # ── Links ─────────────────────────────────────────────────────
                if ws.get('links'):
                    st.markdown("### Links")
                    new_links = []
                    for i, ent in enumerate(ws['links']):
                        ws['links'][i] = {**ent, 'entity_type': 'LNK'}
                        updated, do_del = _render_entity_card(
                            ws, 'links', i, source_cols)
                        if not do_del:
                            new_links.append(updated)
                    new_ws['links'] = new_links

                # ── Satellites ────────────────────────────────────────────────
                if ws.get('satellites'):
                    st.markdown("### Satellites")
                    new_sats = []
                    for i, ent in enumerate(ws['satellites']):
                        et = ent.get('satellite_type', 'SAT')
                        ws['satellites'][i] = {**ent,
                            'entity_type': ent.get('entity_type', et)}
                        updated, do_del = _render_entity_card(
                            ws, 'satellites', i, source_cols)
                        if not do_del:
                            new_sats.append(updated)
                    new_ws['satellites'] = new_sats

                # ── Add new entity ─────────────────────────────────────────────
                st.markdown("---")
                with st.expander("＋ Add New DV Entity"):
                    ae1, ae2 = st.columns(2)
                    new_ename = ae1.text_input(
                        "Entity Name", key="wb_add_name",
                        placeholder="HUB_ACCOUNT")
                    new_etype = ae2.selectbox(
                        "Type", ['HUB', 'LNK', 'SAT', 'MSAT', 'ESAT'],
                        key="wb_add_type")
                    if st.button("Add Entity", key="wb_add_btn"):
                        if new_ename.strip():
                            std_cols = [
                                {'column_name': 'LOAD_DTS', 'data_type': 'TIMESTAMP_NTZ',
                                 'column_role': 'META', 'source_column': '',
                                 'column_definition': 'Record load timestamp'},
                                {'column_name': 'REC_SRC',  'data_type': 'VARCHAR(100)',
                                 'column_role': 'META', 'source_column': '',
                                 'column_definition': 'Record source system'},
                                {'column_name': 'BATCH_ID', 'data_type': 'VARCHAR(100)',
                                 'column_role': 'META', 'source_column': '',
                                 'column_definition': 'Batch load identifier'},
                            ]
                            new_entity = {
                                'entity_id':     new_ename.strip().upper(),
                                'entity_type':   new_etype,
                                'logical_name':  '',
                                'domain':        '',
                                'is_new':        True,
                                'confidence':    'LOW',
                                'entity_status': 'NEW',
                                'rationale':     'Manually added by modeler.',
                                'columns':       std_cols,
                            }
                            target = ('hubs' if new_etype == 'HUB'
                                      else 'links' if new_etype == 'LNK'
                                      else 'satellites')
                            new_ws.setdefault(target, []).append(new_entity)
                            st.session_state.wb_workspace = new_ws
                            st.experimental_rerun()

                # ── Save ──────────────────────────────────────────────────────
                st.markdown("---")
                if st.button("💾 Save Workspace",
                             use_container_width=True, key="wb_save"):
                    if ws_id:
                        st.session_state.wb_workspace = new_ws
                        _save_workspace(ws_id, new_ws)
                        st.success("Workspace saved.")
                    else:
                        st.error("No workspace ID.")

                # ── Re-generate with additional instructions ───────────────────
                st.markdown("---")
                st.markdown("**Regenerate with Additional Instructions**")
                st.caption(
                    "Describe changes for the AI: merge/split entities, "
                    "move columns, change primary key, etc. "
                    "These will be appended to the original modeler notes.")
                regen_notes = st.text_area(
                    "", height=100, key="wb_regen_notes",
                    label_visibility="collapsed",
                    placeholder=(
                        "e.g. Collapse SAT_ACCOUNT_DETAILS and SAT_ACCOUNT_STATUS "
                        "into a single satellite. Move CREDIT_SCORE to a separate "
                        "ESAT. The primary key is ACCOUNT_ID, not the composite."
                    ))

                regen_c1, regen_c2 = st.columns(2)
                if regen_c1.button("🔄 Re-generate",
                                   use_container_width=True, key="wb_regen"):
                    _regen_src_key = f"{src_sys}__{src_tbl}"
                    _regen_new = regen_notes.strip()
                    _regen_old = (meta.get('modeler_notes', '') or '').strip()
                    if _regen_new and _regen_old:
                        _regen_modeler = (
                            f"CHANGE REQUEST (OVERRIDE — APPLY EXACTLY AS STATED):\n{_regen_new}"
                            f"\n\nORIGINAL MODELER NOTES (still apply unless contradicted by the change request above):\n{_regen_old}"
                        )
                    elif _regen_new:
                        _regen_modeler = _regen_new
                    else:
                        _regen_modeler = _regen_old
                    _regen_id = None
                    _regen_err2 = None
                    with st.spinner("Calling Cortex AI… this may take 30–60 seconds"):
                        try:
                            if ws_id:
                                session.sql(f"""
                                    UPDATE META.DV_DESIGN_WORKSPACE
                                    SET STATUS = 'SUPERSEDED'
                                    WHERE WORKSPACE_ID = '{ws_id.replace("'","''")}'
                                """).collect()
                            _regen_id = session.call(
                                "META.SP_GENERATE_DV_PROPOSAL",
                                src_tbl, src_sys,
                                meta.get('source_schema'), None, None,
                                _merge_vault_notes(_regen_src_key, _regen_modeler),
                                _wb_model_id
                            )
                        except Exception as e:
                            _regen_err2 = str(e)
                    if _regen_err2:
                        st.error(f"Re-generation failed: {_regen_err2}")
                    elif _regen_id:
                        _clear_wb_widget_state()
                        st.session_state.wb_workspace_id = _regen_id
                        st.session_state.wb_workspace    = None
                        st.session_state.wb_source_cols  = []
                        st.experimental_rerun()

                if regen_c2.button("✅ Approve All",
                                   use_container_width=True, key="wb_approve"):
                    if ws_id:
                        st.session_state.wb_workspace = new_ws
                        _save_workspace(ws_id, new_ws)
                        with st.spinner("Writing to registry…"):
                            try:
                                _approve_workspace(ws_id, new_ws)
                                st.success("All entities approved and written to registry!")
                                st.session_state.wb_workspace = None
                                st.experimental_rerun()
                            except Exception as e:
                                st.error(f"Approval failed: {e}")
                    else:
                        st.error("No workspace ID.")

        # ── TAB 2: Diagram ────────────────────────────────────────────────────
        with tab_diagram:
            if not ws:
                st.info("Generate an AI proposal to see the diagram.")
            else:
                if st.button("📊 Generate Diagram", key="wb_diagram_btn"):
                    dot_code = _generate_dot(ws)
                    try:
                        st.graphviz_chart(dot_code)
                    except Exception as dg_err:
                        st.info(f"Graphviz render failed: {dg_err}")
                    with st.expander("DOT source"):
                        st.code(dot_code, language="text")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 4: Generate Erwin
# ══════════════════════════════════════════════════════════════════════════════

def _topo_sort_entities(entities: list) -> list:
    """
    Topological sort so DDL is emitted in safe creation order:
      HUBs first → LINKs (depend on HUBs) → SATs/MSATs/ESATs (depend on HUBs/LINKs)
    Returns sorted list of entity dicts.
    """
    TYPE_RANK = {"HUB": 0, "LNK": 1, "SAT": 2, "MSAT": 2, "ESAT": 2}
    return sorted(entities, key=lambda e: (
        TYPE_RANK.get(e.get("ENTITY_TYPE", "SAT"), 3),
        e.get("ENTITY_ID", "")
    ))


def _build_entity_ddl(ent: dict, cols: list, target_schema: str,
                       all_hub_ids: set) -> str:
    """
    Generate CREATE TABLE DDL for one DV entity.
    cols: list of dicts with COLUMN_NAME, DATA_TYPE, COLUMN_ROLE, COLUMN_DEFINITION
    """
    eid    = ent["ENTITY_ID"]
    etype  = ent["ENTITY_TYPE"]
    domain = ent.get("DOMAIN", "") or ""
    src    = ent.get("SOURCE_SYSTEM", "") or ""
    comment = (ent.get("LOGICAL_NAME") or eid).replace("'", "''")

    col_lines = []
    pk_cols   = []
    fk_stmts  = []

    for c in cols:
        cname  = c["COLUMN_NAME"]
        ctype  = c.get("DATA_TYPE") or "VARCHAR(255)"
        crole  = c.get("COLUMN_ROLE") or "ATTR"
        cdef   = (c.get("COLUMN_DEFINITION") or "").replace("'", "''")

        # Nullability
        not_null = " NOT NULL" if crole in ("HK", "BK", "HASHDIFF") else ""
        comment_clause = f" COMMENT '{cdef}'" if cdef else ""
        col_lines.append(f"    {cname:<35} {ctype}{not_null}{comment_clause}")

        if crole == "HK":
            pk_cols.append(cname)

        # FK from SAT/LNK hash key back to parent HUB
        if crole == "FK_HK":
            # Guess parent table from column name: HUB_CUSTOMER_HK → HUB_CUSTOMER
            parent_guess = re.sub(r"_HK$", "", cname, flags=re.IGNORECASE)
            if parent_guess in all_hub_ids:
                fk_stmts.append(
                    f"ALTER TABLE {target_schema}.{eid}\n"
                    f"    ADD CONSTRAINT FK_{eid}_{cname}\n"
                    f"    FOREIGN KEY ({cname})\n"
                    f"    REFERENCES {target_schema}.{parent_guess} ({cname});"
                )

    pk_clause = f",\n    CONSTRAINT PK_{eid} PRIMARY KEY ({', '.join(pk_cols)})" if pk_cols else ""

    tbl_comment = f" COMMENT = '{comment}'"
    ddl = (
        f"-- {'='*68}\n"
        f"-- {etype}: {eid}"
        + (f"  |  domain: {domain}" if domain else "")
        + (f"  |  source: {src}" if src else "") + "\n"
        f"-- {'='*68}\n"
        f"CREATE TABLE IF NOT EXISTS {target_schema}.{eid} (\n"
        + ",\n".join(col_lines)
        + pk_clause + "\n"
        f"){tbl_comment};\n"
    )
    return ddl, fk_stmts


def _build_erwin_rows(ent: dict, cols: list) -> list:
    """Return list of dicts for Erwin-compatible Excel export."""
    rows = []
    for c in cols:
        rows.append({
            "Entity Name":      ent["ENTITY_ID"],
            "Entity Type":      ent["ENTITY_TYPE"],
            "Domain":           ent.get("DOMAIN", ""),
            "Source System":    ent.get("SOURCE_SYSTEM", ""),
            "Attribute Name":   c["COLUMN_NAME"],
            "Data Type":        c.get("DATA_TYPE", "VARCHAR(255)"),
            "Role":             c.get("COLUMN_ROLE", "ATTR"),
            "Definition":       c.get("COLUMN_DEFINITION", ""),
            "Nullable":         "N" if c.get("COLUMN_ROLE") in ("HK","BK","HASHDIFF") else "Y",
            "Primary Key":      "Y" if c.get("COLUMN_ROLE") == "HK" else "N",
        })
    return rows


def page_generate_model():
    st.title("Generate Erwin")
    st.caption("Choose source tables with approved DV proposals, set a target schema, then generate DDL.")

    # ── Load approved workspace→entity mapping ────────────────────────────────
    # For each source table, use ONLY the LATEST approved workspace.
    # Extract entity IDs from that workspace's JSON so previous generations
    # don't bleed through.
    src_map = session.sql("""
        WITH LATEST_SYS AS (
            -- For each schema+table, the most recently used source system
            SELECT SOURCE_SCHEMA, SOURCE_TABLE, SOURCE_SYSTEM, SOURCE_DATABASE
            FROM META.DV_PROFILING_RUN
            WHERE STATUS = 'COMPLETED'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY SOURCE_SCHEMA, SOURCE_TABLE
                ORDER BY COMPLETED_AT DESC
            ) = 1
        ),
        LATEST_WS AS (
            -- Latest approved workspace per table, restricted to current source system
            SELECT
                w.SOURCE_TABLE,
                w.SOURCE_SYSTEM,
                w.SOURCE_SCHEMA,
                w.WORKSPACE_JSON,
                w.LAST_MODIFIED,
                COALESCE(ls.SOURCE_DATABASE, w.SOURCE_SCHEMA) AS SRC_DATABASE
            FROM META.DV_DESIGN_WORKSPACE w
            JOIN LATEST_SYS ls
              ON ls.SOURCE_TABLE  = w.SOURCE_TABLE
             AND ls.SOURCE_SCHEMA = w.SOURCE_SCHEMA
             AND ls.SOURCE_SYSTEM = w.SOURCE_SYSTEM
            WHERE w.STATUS = 'APPROVED'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY w.SOURCE_TABLE, w.SOURCE_SYSTEM
                ORDER BY w.LAST_MODIFIED DESC
            ) = 1
        ),
        WS_ENTITIES AS (
            SELECT
                w.SOURCE_TABLE,
                w.SOURCE_SYSTEM,
                w.SOURCE_SCHEMA,
                w.SRC_DATABASE,
                ent.value:entity_id::VARCHAR AS ENTITY_ID
            FROM LATEST_WS w,
            LATERAL FLATTEN(INPUT => ARRAY_CAT(
                ARRAY_CAT(
                    COALESCE(w.WORKSPACE_JSON:hubs,  ARRAY_CONSTRUCT()),
                    COALESCE(w.WORKSPACE_JSON:links, ARRAY_CONSTRUCT())
                ),
                COALESCE(w.WORKSPACE_JSON:satellites, ARRAY_CONSTRUCT())
            )) ent
        )
        SELECT
            we.SRC_DATABASE,
            we.SOURCE_SCHEMA,
            we.SOURCE_TABLE,
            we.SOURCE_SYSTEM,
            e.ENTITY_ID,
            e.ENTITY_TYPE,
            e.LOGICAL_NAME,
            e.SOURCE_SYSTEM AS ENT_SYSTEM,
            TO_CHAR(e.APPROVED_DATE,'YYYY-MM-DD') AS APPROVED_DATE
        FROM WS_ENTITIES we
        JOIN META.DV_ENTITY e
          ON e.ENTITY_ID       = we.ENTITY_ID
         AND e.APPROVAL_STATUS = 'APPROVED'
        ORDER BY we.SRC_DATABASE, we.SOURCE_SCHEMA, we.SOURCE_TABLE, e.ENTITY_ID
    """).to_pandas()

    if src_map.empty:
        st.info("No approved proposals found. Approve a workspace in the Raw Vault Designer first.")
        return

    # ── Selection panel ───────────────────────────────────────────────────────
    sel_col, cfg_col = st.columns([1, 2])

    with sel_col:
        st.subheader("Source Tables")

        # Cascading: Database → Schema → Tables (multi-select)
        db_opts = sorted(src_map["SRC_DATABASE"].dropna().unique().tolist())
        sel_db  = st.selectbox("Database", db_opts, key="gm_db")

        filt1       = src_map[src_map["SRC_DATABASE"] == sel_db]
        schema_opts = sorted(filt1["SOURCE_SCHEMA"].dropna().unique().tolist())
        sel_schemas = st.multiselect("Schema(s)", schema_opts,
                                     default=schema_opts, key="gm_schemas")

        filt2      = filt1[filt1["SOURCE_SCHEMA"].isin(sel_schemas)] if sel_schemas else filt1
        table_opts = sorted(filt2["SOURCE_TABLE"].dropna().unique().tolist())
        sel_tables = st.multiselect("Source Table(s)", table_opts,
                                    default=table_opts, key="gm_tables")

        # Derive approved entity IDs for selected source tables
        filt3    = filt2[filt2["SOURCE_TABLE"].isin(sel_tables)] if sel_tables else filt2
        sel_ents = filt3["ENTITY_ID"].unique().tolist()

        # Show source system badge(s)
        _gm_systems = filt3["SOURCE_SYSTEM"].dropna().unique().tolist() if not filt3.empty else []
        for _gms in _gm_systems:
            _src_system_badge(_gms)
        st.caption(f"{len(sel_tables)} source table(s) → **{len(sel_ents)} Raw Vault entities**")

        # Show entity breakdown
        if sel_ents:
            _TYPE_ICON = {"HUB": "🔵", "LNK": "🟢", "SAT": "🟠", "MSAT": "🟣", "ESAT": "🔴"}
            for _et in ["HUB", "LNK", "SAT", "MSAT", "ESAT"]:
                _cnt = len(filt3[filt3["ENTITY_TYPE"] == _et])
                if _cnt:
                    st.caption(f"{_TYPE_ICON.get(_et,'')} {_et}: {_cnt}")

    with cfg_col:
        st.subheader("Configuration")

        target_schema = st.text_input(
            "Target Schema (for DDL)",
            value="RAW.DV",
            key="gm_schema",
            help="Schema prefix used in CREATE TABLE statements, e.g. RAW.DV or MY_DB.RAW_VAULT"
        )

        st.markdown("---")
        st.markdown("**Export Options**")
        ec1, ec2, ec3 = st.columns(3)

        gen_sql    = ec1.button("⬇ Download .sql",     use_container_width=True, key="gm_sql")
        gen_excel  = ec2.button("📊 Download Erwin Excel", use_container_width=True, key="gm_excel")
        gen_git    = ec3.button("🔀 Push to Git",       use_container_width=True, key="gm_git")

        # ── Git config (shown only when Push to Git clicked) ──────────────────
        if st.session_state.get("gm_show_git"):
            st.markdown("---")
            st.markdown("**Git Repository Settings**")
            gc1, gc2 = st.columns(2)
            git_repo    = gc1.text_input("Repository URL", key="gm_git_repo",
                                         placeholder="https://github.com/org/repo.git")
            git_branch  = gc2.text_input("Branch", value="main", key="gm_git_branch")
            git_token   = st.text_input("Personal Access Token", type="password",
                                         key="gm_git_token",
                                         help="PAT with repo write access")
            git_path    = st.text_input("File path in repo",
                                         value="ddl/raw_vault.sql", key="gm_git_path")
            git_confirm = st.button("✅ Confirm Push", use_container_width=True,
                                    key="gm_git_confirm")
        else:
            git_confirm = False

        if gen_git:
            st.session_state["gm_show_git"] = True
            st.experimental_rerun()

        # ── Generate DDL when any export is triggered ─────────────────────────
        _do_generate = gen_sql or gen_excel or git_confirm

        if _do_generate and sel_ents:
            in_list = ", ".join(f"'{e}'" for e in sel_ents)

            # Build entity → (source_schema, source_table) mapping from src_map
            # so we can join DV_ENTITY_COLUMN.SOURCE_COLUMN against DV_COLUMN_DEFINITIONS
            _ent_src_rows = src_map[src_map["ENTITY_ID"].isin(sel_ents)][
                ["ENTITY_ID", "SOURCE_SCHEMA", "SOURCE_TABLE"]
            ].drop_duplicates(subset=["ENTITY_ID"])

            # Push the mapping into a temp VALUES clause for SQL join
            _ent_src_vals = ", ".join(
                f"('{r.ENTITY_ID}', '{r.SOURCE_SCHEMA}', '{r.SOURCE_TABLE}')"
                for r in _ent_src_rows.itertuples()
            ) if not _ent_src_rows.empty else "('__NONE__','__NONE__','__NONE__')"

            # Load columns + overlay approved definitions in one SQL pass.
            # Join priority: DV_COLUMN_DEFINITIONS (saved/approved in profiling)
            # beats DV_ENTITY_COLUMN.COLUMN_DEFINITION (AI-generated at proposal time).
            # Match on SOURCE_COLUMN (the original source col name) for ATTR/BK,
            # and fall back to COLUMN_NAME match for safety.
            cols_df = session.sql(f"""
                WITH ENT_SRC AS (
                    SELECT col1 AS ENTITY_ID, col2 AS SRC_SCHEMA, col3 AS SRC_TABLE
                    FROM (VALUES {_ent_src_vals}) AS t(col1, col2, col3)
                )
                SELECT
                    ec.ENTITY_ID,
                    ec.COLUMN_NAME,
                    ec.DATA_TYPE,
                    ec.COLUMN_ROLE,
                    ec.ORDINAL_POSITION,
                    CASE
                        WHEN ec.COLUMN_ROLE IN ('ATTR','BK') AND cd.DEFINITION IS NOT NULL
                            THEN cd.DEFINITION
                        ELSE COALESCE(ec.COLUMN_DEFINITION, '')
                    END AS COLUMN_DEFINITION
                FROM META.DV_ENTITY_COLUMN ec
                LEFT JOIN ENT_SRC es
                    ON es.ENTITY_ID = ec.ENTITY_ID
                LEFT JOIN META.DV_COLUMN_DEFINITIONS cd
                    ON  UPPER(cd.SOURCE_SCHEMA) = UPPER(es.SRC_SCHEMA)
                    AND UPPER(cd.SOURCE_TABLE)  = UPPER(es.SRC_TABLE)
                    AND UPPER(cd.COLUMN_NAME)   = UPPER(
                            COALESCE(NULLIF(ec.SOURCE_COLUMN,''), ec.COLUMN_NAME))
                WHERE ec.ENTITY_ID IN ({in_list})
                ORDER BY ec.ENTITY_ID, ec.ORDINAL_POSITION
            """).to_pandas()

            # Build entity list filtered + sorted
            # Load full entity details for selected IDs
            _full_ent_df = session.sql(f"""
                SELECT ENTITY_ID, ENTITY_TYPE, LOGICAL_NAME, SOURCE_SYSTEM,
                       '' AS DOMAIN,
                       TO_CHAR(APPROVED_DATE,'YYYY-MM-DD') AS APPROVED_DATE
                FROM META.DV_ENTITY
                WHERE ENTITY_ID IN ({in_list})
            """).to_pandas()
            sel_ent_rows = _full_ent_df.to_dict("records")
            sorted_ents  = _topo_sort_entities(sel_ent_rows)
            all_hub_ids  = {e["ENTITY_ID"] for e in sel_ent_rows if e["ENTITY_TYPE"] == "HUB"}

            # Build DDL
            ddl_parts  = []
            fk_parts   = []
            erwin_rows = []

            ddl_parts.append(
                f"-- NEXUS DV2.0 — Raw Vault DDL\n"
                f"-- Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}\n"
                f"-- Entities: {len(sorted_ents)}\n"
                f"-- Schema: {target_schema}\n"
                f"-- Order: HUBs → LINKs → SATs  (FK constraints appended last)\n"
                f"-- {'='*68}\n\n"
            )

            for ent in sorted_ents:
                eid  = ent["ENTITY_ID"]
                ecols = cols_df[cols_df["ENTITY_ID"] == eid].to_dict("records")
                ddl, fks = _build_entity_ddl(ent, ecols, target_schema, all_hub_ids)
                ddl_parts.append(ddl)
                fk_parts.extend(fks)
                erwin_rows.extend(_build_erwin_rows(ent, ecols))

            if fk_parts:
                ddl_parts.append(
                    "\n-- " + "="*68 + "\n"
                    "-- FOREIGN KEY CONSTRAINTS (added after all tables exist)\n"
                    "-- " + "="*68 + "\n\n"
                    + "\n\n".join(fk_parts) + "\n"
                )

            full_ddl = "\n".join(ddl_parts)

            # ── Preview ───────────────────────────────────────────────────────
            with st.expander("Preview DDL", expanded=True):
                st.code(full_ddl[:6000] +
                        ("\n\n-- [truncated for preview — full file in download]"
                         if len(full_ddl) > 6000 else ""),
                        language="sql")

            # ── .sql: display in code block (copy button built-in) ───────────
            if gen_sql:
                st.markdown("---")
                st.markdown("**📋 SQL DDL — copy with the button top-right of the block**")
                st.caption(
                    "Paste directly into a Snowflake worksheet, SnowSQL, or any SQL editor. "
                    "Tables are ordered HUBs → LINKs → SATs; FK constraints are last.")
                st.code(full_ddl, language="sql")

                # Also persist to a Snowflake table so it can be retrieved later
                _ts_str = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
                _safe_ddl = full_ddl.replace("'", "''")
                try:
                    session.sql(f"""
                        CREATE TABLE IF NOT EXISTS META.DV_DDL_EXPORT (
                            EXPORT_ID     VARCHAR     DEFAULT UUID_STRING(),
                            EXPORTED_AT   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                            EXPORTED_BY   VARCHAR     DEFAULT CURRENT_USER(),
                            ENTITY_COUNT  NUMBER,
                            TARGET_SCHEMA VARCHAR,
                            DDL_TEXT      VARCHAR
                        )
                    """).collect()
                    session.sql(f"""
                        INSERT INTO META.DV_DDL_EXPORT
                            (ENTITY_COUNT, TARGET_SCHEMA, DDL_TEXT)
                        VALUES ({len(sorted_ents)}, '{target_schema}', '{_safe_ddl}')
                    """).collect()
                    st.caption(
                        "DDL also saved to **NEXUS.META.DV_DDL_EXPORT** — "
                        "query it anytime to retrieve previous exports.")
                except Exception:
                    pass  # table save is best-effort

            # ── Erwin export: write to queryable Snowflake table ─────────────
            if gen_excel and erwin_rows:
                erwin_df = pd.DataFrame(erwin_rows)
                _ts_str  = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
                _export_id = f"ERWIN_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}"

                try:
                    session.sql("""
                        CREATE TABLE IF NOT EXISTS META.DV_ERWIN_EXPORT (
                            EXPORT_ID       VARCHAR,
                            EXPORTED_AT     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                            EXPORTED_BY     VARCHAR       DEFAULT CURRENT_USER(),
                            ENTITY_NAME     VARCHAR,
                            ENTITY_TYPE     VARCHAR,
                            DOMAIN          VARCHAR,
                            SOURCE_SYSTEM   VARCHAR,
                            ATTRIBUTE_NAME  VARCHAR,
                            DATA_TYPE       VARCHAR,
                            ROLE            VARCHAR,
                            DEFINITION      VARCHAR,
                            NULLABLE        VARCHAR,
                            PRIMARY_KEY     VARCHAR
                        )
                    """).collect()

                    # Bulk insert via temp table from pandas
                    _sp_df = session.create_dataframe(erwin_df.rename(columns={
                        "Entity Name":   "ENTITY_NAME",
                        "Entity Type":   "ENTITY_TYPE",
                        "Domain":        "DOMAIN",
                        "Source System": "SOURCE_SYSTEM",
                        "Attribute Name":"ATTRIBUTE_NAME",
                        "Data Type":     "DATA_TYPE",
                        "Role":          "ROLE",
                        "Definition":    "DEFINITION",
                        "Nullable":      "NULLABLE",
                        "Primary Key":   "PRIMARY_KEY",
                    }))
                    _sp_df \
                        .with_column("EXPORT_ID",   F.lit(_export_id)) \
                        .with_column("EXPORTED_AT", F.current_timestamp()) \
                        .with_column("EXPORTED_BY", F.current_user()) \
                        .select("EXPORT_ID","EXPORTED_AT","EXPORTED_BY",
                                "ENTITY_NAME","ENTITY_TYPE","DOMAIN","SOURCE_SYSTEM",
                                "ATTRIBUTE_NAME","DATA_TYPE","ROLE","DEFINITION",
                                "NULLABLE","PRIMARY_KEY") \
                        .write.mode("append").save_as_table("META.DV_ERWIN_EXPORT")

                    st.success(
                        f"Erwin data written to **NEXUS.META.DV_ERWIN_EXPORT** "
                        f"(EXPORT_ID = `{_export_id}`, {len(erwin_rows)} rows).")
                    st.code(
                        f"SELECT * FROM NEXUS.META.DV_ERWIN_EXPORT\n"
                        f"WHERE EXPORT_ID = '{_export_id}'\nORDER BY ENTITY_NAME, ATTRIBUTE_NAME;",
                        language="sql")
                    st.caption(
                        "Run the query above in Snowsight → Results → Download as CSV/Excel.")

                    # Also show inline so modeler can review before exporting
                    st.markdown("**Preview**")
                    st.dataframe(erwin_df.reset_index(drop=True),
                                 use_container_width=True)
                except Exception as _xe:
                    st.error(f"Erwin export failed: {_xe}")
                    st.dataframe(erwin_df.reset_index(drop=True),
                                 use_container_width=True)

            # ── Push to Git ───────────────────────────────────────────────────
            if git_confirm:
                import base64, urllib.request, urllib.error
                git_repo_url = st.session_state.get("gm_git_repo", "").strip()
                git_tok      = st.session_state.get("gm_git_token", "").strip()
                git_br       = st.session_state.get("gm_git_branch", "main").strip()
                git_fp       = st.session_state.get("gm_git_path", "ddl/raw_vault.sql").strip()

                if not git_repo_url or not git_tok:
                    st.error("Provide both Repository URL and Personal Access Token.")
                else:
                    # Convert https://github.com/org/repo.git → API path
                    _api_base = re.sub(
                        r"https://github\.com/(.+?)(?:\.git)?$",
                        r"https://api.github.com/repos/\1", git_repo_url)
                    _api_url  = f"{_api_base}/contents/{git_fp}"

                    # Check if file exists (to get SHA for update)
                    _sha = None
                    try:
                        _req = urllib.request.Request(
                            _api_url,
                            headers={"Authorization": f"token {git_tok}",
                                     "Accept": "application/vnd.github.v3+json"})
                        with urllib.request.urlopen(_req) as _resp:
                            _sha = json.loads(_resp.read())["sha"]
                    except urllib.error.HTTPError as _he:
                        if _he.code != 404:
                            st.error(f"Git API error checking file: {_he}")

                    # PUT to create or update
                    _payload = {
                        "message": f"NEXUS DV2.0: update raw vault DDL ({len(sorted_ents)} entities)",
                        "branch":  git_br,
                        "content": base64.b64encode(full_ddl.encode()).decode(),
                    }
                    if _sha:
                        _payload["sha"] = _sha

                    try:
                        _put_data = json.dumps(_payload).encode("utf-8")
                        _put_req  = urllib.request.Request(
                            _api_url, data=_put_data, method="PUT",
                            headers={"Authorization": f"token {git_tok}",
                                     "Content-Type": "application/json",
                                     "Accept": "application/vnd.github.v3+json"})
                        with urllib.request.urlopen(_put_req) as _resp:
                            _result = json.loads(_resp.read())
                        st.success(
                            f"Pushed to `{git_repo_url}` "
                            f"branch `{git_br}` → `{git_fp}`\n\n"
                            f"Commit: {_result.get('commit',{}).get('sha','')[:7]}")
                        st.session_state["gm_show_git"] = False
                    except Exception as _pe:
                        st.error(f"Git push failed: {_pe}")

        elif _do_generate and not sel_ents:
            st.warning("Select at least one entity.")

        # ── Entity summary table (always visible once selection made) ─────────
        if sel_ents:
            st.markdown("---")
            st.markdown("**Raw Vault tables for selected source tables**")
            _TYPE_ICON = {"HUB": "🔵", "LNK": "🟢", "SAT": "🟠",
                          "MSAT": "🟣", "ESAT": "🔴"}
            _disp = filt3[["ENTITY_ID","ENTITY_TYPE","LOGICAL_NAME",
                            "SOURCE_TABLE","APPROVED_DATE"]].drop_duplicates().copy()
            _disp["Type"] = _disp["ENTITY_TYPE"].map(
                lambda t: f"{_TYPE_ICON.get(t,'')} {t}")
            st.dataframe(
                _disp[["Type","ENTITY_ID","LOGICAL_NAME","SOURCE_TABLE","APPROVED_DATE"]]
                .rename(columns={
                    "ENTITY_ID":    "Raw Vault Table",
                    "LOGICAL_NAME": "Logical Name",
                    "SOURCE_TABLE": "Source Table",
                    "APPROVED_DATE":"Approved",
                })
                .sort_values(["Type","Raw Vault Table"])
                .reset_index(drop=True),
                use_container_width=True
            )


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 5: Generate DBT
# ══════════════════════════════════════════════════════════════════════════════

def page_generate_dbt():
    st.title("Generate DBT")
    st.caption(
        "Generate all dbt model files needed to load your approved Raw Vault using automate_dv. "
        "Copy each file into your NexusDBT project in the Snowflake dbt IDE."
    )

    # ── helpers ──────────────────────────────────────────────────────────────
    def _code_tab(label: str, content: str, lang: str = "yaml"):
        st.markdown(f"**`{label}`**")
        st.code(content, language=lang)

    def _indent(text: str, spaces: int = 2) -> str:
        pad = " " * spaces
        return "\n".join(pad + l for l in text.splitlines())

    # ══════════════════════════════════════════════════════════════════════════
    # SECTION 1 — ONE-TIME PROJECT SETUP FILES
    # ══════════════════════════════════════════════════════════════════════════
    with st.expander("📁 One-Time Project Setup Files  (copy these once into your NexusDBT project)", expanded=True):
        st.caption(
            "These files configure your dbt project. If you already have them set up, "
            "skip this section. Otherwise copy each file into the root of your NexusDBT project."
        )
        setup_tabs = st.tabs(["dbt_project.yml", "packages.yml", "profiles.yml"])

        with setup_tabs[0]:
            _code_tab("dbt_project.yml", """\
name: 'nexusdbt'
version: '1.0.0'
config-version: 2

profile: 'nexusdbt'

model-paths: ["models"]
seed-paths:  ["seeds"]
test-paths:  ["tests"]
macro-paths: ["macros"]

target-path:  "target"
clean-targets: ["target", "dbt_packages"]

models:
  nexusdbt:
    staging:
      +materialized: view
      +schema: STG
    raw_vault:
      +materialized: incremental
      +schema: RAW_VAULT
""")
            st.info(
                "Staging models use `MD5_BINARY()` directly — this matches the `BINARY(16)` "
                "type that automate_dv's hub/sat/link macros create in target tables. No extra vars needed."
            )

        with setup_tabs[1]:
            _code_tab("packages.yml", """\
packages:
  - package: Datavault-UK/automate_dv
    version: [">=0.10.0", "<1.0.0"]
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
  - package: calogica/dbt_date
    version: [">=0.10.0", "<1.0.0"]
  - package: calogica/dbt_expectations
    version: [">=0.10.0", "<1.0.0"]
""")
            st.info("After copying, run `dbt deps` in the dbt IDE to install the packages.")

        with setup_tabs[2]:
            _code_tab("profiles.yml", """\
nexusdbt:
  target: dev
  outputs:
    dev:
      type:          snowflake
      account:       "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user:          "{{ env_var('SNOWFLAKE_USER') }}"
      authenticator: oauth
      token:         "{{ env_var('SNOWFLAKE_TOKEN') }}"
      role:          ACCOUNTADMIN
      database:      NEXUS
      warehouse:     COMPUTE_WH
      schema:        RAW_VAULT
      threads:       4
""")
            st.info(
                "Snowflake's native dbt IDE requires this file to exist but injects "
                "`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER` and `SNOWFLAKE_TOKEN` automatically — "
                "you do not need to fill in any values yourself. Just copy as-is."
            )


    st.markdown("---")

    # ══════════════════════════════════════════════════════════════════════════
    # SHARED: Load approved workspaces (used by both sections below)
    # ══════════════════════════════════════════════════════════════════════════
    approved_df = session.sql("""
        WITH LATEST_SYS AS (
            SELECT SOURCE_SCHEMA, SOURCE_TABLE, SOURCE_SYSTEM
            FROM META.DV_PROFILING_RUN
            WHERE STATUS = 'COMPLETED'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY SOURCE_SCHEMA, SOURCE_TABLE
                ORDER BY COMPLETED_AT DESC
            ) = 1
        )
        SELECT
            w.SOURCE_TABLE,
            w.SOURCE_SYSTEM,
            w.SOURCE_SCHEMA,
            COALESCE(r.SOURCE_DATABASE, '?') AS SRC_DATABASE,
            w.WORKSPACE_ID
        FROM META.DV_DESIGN_WORKSPACE w
        JOIN LATEST_SYS ls
          ON ls.SOURCE_TABLE  = w.SOURCE_TABLE
         AND ls.SOURCE_SCHEMA = w.SOURCE_SCHEMA
         AND ls.SOURCE_SYSTEM = w.SOURCE_SYSTEM
        LEFT JOIN META.DV_PROFILING_RUN r
               ON r.SOURCE_TABLE  = w.SOURCE_TABLE
              AND r.SOURCE_SYSTEM = w.SOURCE_SYSTEM
              AND r.STATUS        = 'COMPLETED'
        WHERE w.STATUS = 'APPROVED'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY w.SOURCE_TABLE, w.SOURCE_SYSTEM
            ORDER BY w.LAST_MODIFIED DESC
        ) = 1
        ORDER BY SRC_DATABASE, w.SOURCE_SCHEMA, w.SOURCE_TABLE
    """).to_pandas()

    if approved_df.empty:
        st.info("No approved workspaces found. Approve a model in Design Raw Vault first.")
        return

    _TYPE_RANK = {"HUB": 0, "LNK": 1, "SAT": 2, "MSAT": 2, "ESAT": 2}

    # ── Shared helper: load all entity data for one approved row ─────────────
    def _load_table_data(a_tbl, a_sys, a_sch):
        """Returns (eid_list, ent_df, col_df, hash_df, saved_defs) for one approved table."""
        ws_ents = session.sql(f"""
            WITH LATEST_WS AS (
                SELECT WORKSPACE_JSON
                FROM META.DV_DESIGN_WORKSPACE
                WHERE SOURCE_TABLE  = '{a_tbl.replace("'","''")}'
                  AND SOURCE_SYSTEM = '{a_sys.replace("'","''")}'
                  AND STATUS        = 'APPROVED'
                ORDER BY LAST_MODIFIED DESC
                LIMIT 1
            )
            SELECT ent.value:entity_id::VARCHAR AS ENTITY_ID
            FROM LATEST_WS,
            LATERAL FLATTEN(INPUT => ARRAY_CAT(
                ARRAY_CAT(
                    COALESCE(WORKSPACE_JSON:hubs,       ARRAY_CONSTRUCT()),
                    COALESCE(WORKSPACE_JSON:links,      ARRAY_CONSTRUCT())
                ),
                COALESCE(WORKSPACE_JSON:satellites, ARRAY_CONSTRUCT())
            )) ent
        """).collect()
        eid_list = [r["ENTITY_ID"] for r in ws_ents if r["ENTITY_ID"]]
        if not eid_list:
            return eid_list, None, None, None, {}

        in_list = ", ".join(f"'{e}'" for e in eid_list)

        ent_df_t = session.sql(f"""
            SELECT ENTITY_ID, ENTITY_TYPE, LOGICAL_NAME, SOURCE_SYSTEM, PARENT_ENTITY_ID
            FROM META.DV_ENTITY
            WHERE ENTITY_ID IN ({in_list}) AND APPROVAL_STATUS = 'APPROVED'
        """).to_pandas()

        # Load columns with approved definitions overlaid in SQL.
        # Matches on SOURCE_COLUMN (original source col name) so definitions
        # saved in profiling flow into satellites, hubs, and links correctly.
        col_df_t = session.sql(f"""
            SELECT
                ec.ENTITY_ID,
                ec.COLUMN_NAME,
                ec.DATA_TYPE,
                ec.COLUMN_ROLE,
                ec.ORDINAL_POSITION,
                ec.SOURCE_COLUMN,
                CASE
                    WHEN ec.COLUMN_ROLE IN ('ATTR','BK') AND cd.DEFINITION IS NOT NULL
                        THEN cd.DEFINITION
                    ELSE COALESCE(ec.COLUMN_DEFINITION, '')
                END AS COLUMN_DEFINITION
            FROM META.DV_ENTITY_COLUMN ec
            LEFT JOIN META.DV_COLUMN_DEFINITIONS cd
                ON  UPPER(cd.SOURCE_SCHEMA) = UPPER('{a_sch.replace("'","''")}')
                AND UPPER(cd.SOURCE_TABLE)  = UPPER('{a_tbl.replace("'","''")}')
                AND UPPER(cd.COLUMN_NAME)   = UPPER(
                        COALESCE(NULLIF(ec.SOURCE_COLUMN,''), ec.COLUMN_NAME))
            WHERE ec.ENTITY_ID IN ({in_list})
            ORDER BY ec.ENTITY_ID, ec.ORDINAL_POSITION
        """).to_pandas()

        hash_df_t = session.sql(f"""
            SELECT ENTITY_ID, HASH_KEY_NAME, HASH_TYPE,
                   SOURCE_COLUMNS::VARCHAR AS SOURCE_COLUMNS_JSON,
                   NULL_REPLACEMENT, DELIMITER, ALGORITHM
            FROM META.DV_HASH_DEFINITION
            WHERE ENTITY_ID IN ({in_list})
        """).to_pandas()

        # saved_defs_t still used for schema.yml descriptions (dbt page)
        saved_defs_t = {}
        try:
            _dr = session.sql(f"""
                SELECT COLUMN_NAME, DEFINITION
                FROM META.DV_COLUMN_DEFINITIONS
                WHERE UPPER(SOURCE_SCHEMA) = UPPER('{a_sch.replace("'","''")}')
                  AND UPPER(SOURCE_TABLE)  = UPPER('{a_tbl.replace("'","''")}')
            """).collect()
            for r in _dr:
                if r["DEFINITION"]:
                    saved_defs_t[r["COLUMN_NAME"].upper()] = r["DEFINITION"]
        except Exception:
            pass

        return eid_list, ent_df_t, col_df_t, hash_df_t, saved_defs_t

    # ── Shared helper: build hash lookup from hash_df ────────────────────────
    def _build_hash_lkp(hash_df_t):
        lkp = {}
        for _, hr in hash_df_t.iterrows():
            eid  = hr["ENTITY_ID"]
            hkey = hr["HASH_KEY_NAME"]
            try:
                cols = json.loads(hr["SOURCE_COLUMNS_JSON"])
            except Exception:
                cols = []
            lkp.setdefault(eid, {})[hkey] = cols
        return lkp

    # ── Shared helper: build staging SQL for one table ───────────────────────
    def _md5_expr(cols: list) -> str:
        parts = [f"UPPER(TRIM(CAST(src.{c} AS VARCHAR)))" for c in cols]
        concat = "\n        || '||' ||\n        ".join(parts)
        if len(cols) == 1:
            return f"MD5_BINARY({parts[0]})"
        return f"MD5_BINARY(\n        {concat}\n    )"

    def _build_stg_sql(stg_name, src_system, sel_tbl, hash_lkp):
        stg_select_lines = ["    src.*"]
        seen = set()
        for _eid, hkeys in hash_lkp.items():
            for hkey, src_cols in hkeys.items():
                if hkey in seen:
                    continue
                seen.add(hkey)
                stg_select_lines.append(f"    {_md5_expr(src_cols)} AS {hkey}")
        stg_select_lines.append("    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS LOAD_DTS")
        stg_select_lines.append(f"    '{src_system}'                    AS REC_SRC")
        stg_select_lines.append("    TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDHH24MISS') AS BATCH_ID")
        return (
            "{{- config(materialized='view', schema='STG') -}}\n\n"
            "SELECT\n"
            + ",\n".join(stg_select_lines) + "\n\n"
            f"FROM {{{{ source('{src_system.lower()}', '{sel_tbl}') }}}} AS src\n"
        )

    # ── Shared helper: build vault entity SQL files for one table ────────────
    def _build_entity_files(ents_sorted, col_df_t, hash_lkp, stg_name):
        entity_files = {}
        for ent in ents_sorted:
            eid   = ent["ENTITY_ID"]
            etype = ent["ENTITY_TYPE"]
            ecols = col_df_t[col_df_t["ENTITY_ID"] == eid].to_dict("records")
            hk_cols   = [c for c in ecols if c["COLUMN_ROLE"] == "HK"]
            bk_cols   = [c for c in ecols if c["COLUMN_ROLE"] == "BK"]
            fk_cols   = [c for c in ecols if c["COLUMN_ROLE"] == "FK_HK"]
            attr_cols = [c for c in ecols if c["COLUMN_ROLE"] == "ATTR"]
            hd_cols   = [c for c in ecols if c["COLUMN_ROLE"] == "HASHDIFF"]
            mak_cols  = [c for c in ecols if c["COLUMN_ROLE"] == "MAK"]
            hk_name = hk_cols[0]["COLUMN_NAME"] if hk_cols else f"{eid}_HK"
            hd_name = hd_cols[0]["COLUMN_NAME"] if hd_cols else f"{eid}_HASHDIFF"
            src_payload = [c["COLUMN_NAME"] for c in attr_cols]

            if etype == "HUB":
                nk_list = [c["COLUMN_NAME"] for c in bk_cols]
                nk_val  = f"'{nk_list[0]}'" if len(nk_list) == 1 else "[" + ", ".join(f"'{n}'" for n in nk_list) + "]"
                sql = (
                    "{{- config(\n    materialized = 'incremental',\n    schema       = 'RAW_VAULT',\n"
                    f"    unique_key   = '{hk_name}'\n) -}}\n\n"
                    f"{{% set source_model = '{stg_name}' %}}\n"
                    f"{{% set src_pk       = '{hk_name}' %}}\n"
                    f"{{% set src_nk       = {nk_val} %}}\n"
                    "{% set src_ldts     = 'LOAD_DTS' %}\n{% set src_source   = 'REC_SRC' %}\n\n"
                    "{{ automate_dv.hub(src_pk=src_pk, src_nk=src_nk,\n"
                    "                   src_ldts=src_ldts, src_source=src_source,\n"
                    "                   source_model=source_model) }}\n"
                )
            elif etype == "LNK":
                fk_list = [c["COLUMN_NAME"] for c in fk_cols]
                fk_val  = f"'{fk_list[0]}'" if len(fk_list) == 1 else "[" + ", ".join(f"'{f}'" for f in fk_list) + "]"
                sql = (
                    "{{- config(\n    materialized = 'incremental',\n    schema       = 'RAW_VAULT',\n"
                    f"    unique_key   = '{hk_name}'\n) -}}\n\n"
                    f"{{% set source_model = '{stg_name}' %}}\n"
                    f"{{% set src_pk       = '{hk_name}' %}}\n"
                    f"{{% set src_fk       = {fk_val} %}}\n"
                    "{% set src_ldts     = 'LOAD_DTS' %}\n{% set src_source   = 'REC_SRC' %}\n\n"
                    "{{ automate_dv.link(src_pk=src_pk, src_fk=src_fk,\n"
                    "                    src_ldts=src_ldts, src_source=src_source,\n"
                    "                    source_model=source_model) }}\n"
                )
            elif etype in ("SAT", "ESAT"):
                parent_hk_col = fk_cols[0]["COLUMN_NAME"] if fk_cols else hk_name
                payload_val   = "[" + ", ".join(f"'{p}'" for p in src_payload) + "]" if src_payload else "[]"
                sql = (
                    "{{- config(\n    materialized = 'incremental',\n    schema       = 'RAW_VAULT',\n"
                    f"    unique_key   = ['{parent_hk_col}', '{hd_name}']\n) -}}\n\n"
                    f"{{% set source_model = '{stg_name}' %}}\n"
                    f"{{% set src_pk       = '{parent_hk_col}' %}}\n"
                    f"{{% set src_hashdiff = '{hd_name}' %}}\n"
                    f"{{% set src_payload  = {payload_val} %}}\n"
                    "{% set src_ldts     = 'LOAD_DTS' %}\n{% set src_source   = 'REC_SRC' %}\n\n"
                    "{{ automate_dv.sat(src_pk=src_pk, src_hashdiff=src_hashdiff,\n"
                    "                   src_payload=src_payload,\n"
                    "                   src_ldts=src_ldts, src_source=src_source,\n"
                    "                   source_model=source_model) }}\n"
                )
            elif etype == "MSAT":
                parent_hk_col = fk_cols[0]["COLUMN_NAME"] if fk_cols else hk_name
                mak_list  = [c["COLUMN_NAME"] for c in mak_cols]
                mak_val   = f"'{mak_list[0]}'" if len(mak_list) == 1 else "[" + ", ".join(f"'{m}'" for m in mak_list) + "]"
                payload_val = "[" + ", ".join(f"'{p}'" for p in src_payload) + "]" if src_payload else "[]"
                sql = (
                    "{{- config(\n    materialized = 'incremental',\n    schema       = 'RAW_VAULT',\n"
                    f"    unique_key   = ['{parent_hk_col}', '{hd_name}', 'LOAD_DTS']\n) -}}\n\n"
                    f"{{% set source_model = '{stg_name}' %}}\n"
                    f"{{% set src_pk       = '{parent_hk_col}' %}}\n"
                    f"{{% set src_hashdiff = '{hd_name}' %}}\n"
                    f"{{% set src_payload  = {payload_val} %}}\n"
                    "{% set src_ldts     = 'LOAD_DTS' %}\n{% set src_source   = 'REC_SRC' %}\n"
                    f"{{% set src_mak     = {mak_val} %}}\n\n"
                    "{{ automate_dv.ma_sat(src_pk=src_pk, src_cdk=src_mak,\n"
                    "                      src_hashdiff=src_hashdiff,\n"
                    "                      src_payload=src_payload,\n"
                    "                      src_ldts=src_ldts, src_source=src_source,\n"
                    "                      source_model=source_model) }}\n"
                )
            else:
                sql = f"-- Unknown entity type {etype} for {eid}\n"
            entity_files[f"{eid}.sql"] = sql
        return entity_files

    # ══════════════════════════════════════════════════════════════════════════
    # SECTION 2 — COMBINED YAML FILES  (sources.yml + schema.yml for ALL tables)
    # ══════════════════════════════════════════════════════════════════════════
    st.subheader("Step 2 — Generate Combined YAML Files")
    st.caption(
        "**sources.yml** and **schema.yml** cover every approved source table at once — "
        "one file per type, no manual merging needed. Regenerate this whenever you approve a new table."
    )

    n_approved = len(approved_df)
    tbl_summary = ", ".join(
        f"{r['SOURCE_SYSTEM']}/{r['SOURCE_TABLE']}"
        for _, r in approved_df.iterrows()
    )
    st.info(f"{n_approved} approved table(s): {tbl_summary}")

    if st.button(
        f"📋 Generate sources.yml + schema.yml  ({n_approved} table(s))",
        type="primary", key="dbt_all_yaml_btn"
    ):
        # Build combined sources.yml and schema.yml across all approved tables
        # source_groups: (sys, db, schema) → [{"table": ..., "src_cols": df, "saved_defs": {}}]
        source_groups  = {}
        schema_entries = []   # list of dicts for schema.yml

        all_stg_names = []  # track for folder preview

        with st.spinner("Loading entity data for all approved tables…"):
            for _, arow in approved_df.iterrows():
                a_tbl = arow["SOURCE_TABLE"]
                a_sys = arow["SOURCE_SYSTEM"]
                a_sch = arow["SOURCE_SCHEMA"]
                a_db  = arow["SRC_DATABASE"]

                eid_list, ent_df_t, col_df_t, hash_df_t, saved_defs_t = \
                    _load_table_data(a_tbl, a_sys, a_sch)
                if not eid_list or ent_df_t is None:
                    st.warning(f"No entities found for {a_sys}/{a_tbl} — skipping.")
                    continue

                # Group tables by (source_system, database, schema) for sources.yml
                grp_key = (a_sys, a_db, a_sch)
                if grp_key not in source_groups:
                    source_groups[grp_key] = []
                # Source columns = columns belonging to the first entity (hub/satellite drives the column list)
                first_eid_cols = col_df_t[col_df_t["ENTITY_ID"] == eid_list[0]]
                source_groups[grp_key].append({
                    "table":      a_tbl,
                    "src_cols":   first_eid_cols,
                    "saved_defs": saved_defs_t,
                })

                stg_name_t    = f"stg_{a_sys.lower()}_{a_tbl.lower()}"
                ents_sorted_t = sorted(
                    ent_df_t.to_dict("records"),
                    key=lambda e: (_TYPE_RANK.get(e["ENTITY_TYPE"], 3), e["ENTITY_ID"])
                )
                all_stg_names.append(stg_name_t)
                schema_entries.append({
                    "stg_name":   stg_name_t,
                    "src_system": a_sys,
                    "table":      a_tbl,
                    "eid_list":   eid_list,
                    "ents_sorted": ents_sorted_t,
                    "col_df":     col_df_t,
                    "saved_defs": saved_defs_t,
                })

        # ── Build combined sources.yml ────────────────────────────────────────
        combined_src = "version: 2\n\nsources:\n"
        for (a_sys, a_db, a_sch), tables in source_groups.items():
            combined_src += f"  - name: {a_sys.lower()}\n"
            combined_src += "    description: >\n"
            combined_src += f"      {a_sys} — profiled and modelled in NEXUS DV2.0.\n"
            combined_src += f"    database: {a_db}\n"
            combined_src += f"    schema:   {a_sch}\n"
            combined_src += "    tables:\n"
            for tbl_info in tables:
                combined_src += f"      - name: {tbl_info['table']}\n"
                combined_src += "        columns:\n"
                for _, cr in tbl_info["src_cols"].iterrows():
                    defn = tbl_info["saved_defs"].get(cr["COLUMN_NAME"].upper(), "")
                    combined_src += f"          - name: {cr['COLUMN_NAME']}\n"
                    if defn:
                        combined_src += f"            description: \"{defn}\"\n"
            combined_src += "\n"

        # ── Build combined schema.yml ─────────────────────────────────────────
        combined_schema = "version: 2\n\nmodels:\n"
        for entry in schema_entries:
            stg_n   = entry["stg_name"]
            a_sys   = entry["src_system"]
            a_tbl   = entry["table"]
            col_df_e = entry["col_df"]
            eid_list_e = entry["eid_list"]
            saved_d = entry["saved_defs"]

            # Staging model entry
            combined_schema += f"  - name: {stg_n}\n"
            combined_schema += "    description: >\n"
            combined_schema += f"      Staging model for {a_sys} / {a_tbl}.\n"
            combined_schema += "    columns:\n"
            combined_schema += "      - name: LOAD_DTS\n"
            combined_schema += "        description: Timestamp when this record was loaded.\n"
            combined_schema += "      - name: REC_SRC\n"
            combined_schema += f"        description: Source system — always '{a_sys}'.\n"
            combined_schema += "      - name: BATCH_ID\n"
            combined_schema += "        description: Batch load identifier (YYYYMMDDHH24MISS).\n"
            if eid_list_e:
                for _, cr in col_df_e[col_df_e["ENTITY_ID"] == eid_list_e[0]].iterrows():
                    defn = saved_d.get(cr["COLUMN_NAME"].upper(), "")
                    combined_schema += f"      - name: {cr['COLUMN_NAME']}\n"
                    if defn:
                        combined_schema += f"        description: \"{defn}\"\n"
            combined_schema += "\n"

            # Vault entity entries
            for ent in entry["ents_sorted"]:
                eid   = ent["ENTITY_ID"]
                etype = ent["ENTITY_TYPE"]
                lname = ent.get("LOGICAL_NAME") or eid
                ecols = col_df_e[col_df_e["ENTITY_ID"] == eid].to_dict("records")
                combined_schema += f"  - name: {eid}\n"
                combined_schema += "    description: >\n"
                combined_schema += f"      {etype} entity — {lname}. Source: {a_sys} / {a_tbl}.\n"
                combined_schema += "    columns:\n"
                for c in ecols:
                    defn = (c.get("COLUMN_DEFINITION") or "").strip()
                    if c["COLUMN_ROLE"] == "ATTR":
                        defn = saved_d.get(c["COLUMN_NAME"].upper(), defn)
                    combined_schema += f"      - name: {c['COLUMN_NAME']}\n"
                    if defn:
                        combined_schema += f"        description: \"{defn.replace(chr(34), chr(39))}\"\n"
                combined_schema += "\n"

        # ── Render ────────────────────────────────────────────────────────────
        yaml_tabs = st.tabs(["sources.yml", "schema.yml"])
        with yaml_tabs[0]:
            st.caption("📁 Place this file in `models/staging/sources.yml`")
            _code_tab("sources.yml", combined_src, lang="yaml")
        with yaml_tabs[1]:
            st.caption("📁 Place this file in `models/raw_vault/schema.yml`")
            _code_tab("schema.yml", combined_schema, lang="yaml")

    st.markdown("---")

    # ══════════════════════════════════════════════════════════════════════════
    # SECTION 3 — SQL MODELS (per source table)
    # ══════════════════════════════════════════════════════════════════════════
    st.subheader("Step 3 — Generate SQL Models for a Source Table")
    st.caption(
        "Select one approved source table to generate its staging view and vault entity SQL files. "
        "Repeat for each source table."
    )

    sel_col, _ = st.columns([1, 2])
    with sel_col:
        db_opts = sorted(approved_df["SRC_DATABASE"].dropna().unique().tolist())
        sel_db  = st.selectbox("Source Database", db_opts, key="dbt_db")

        f1 = approved_df[approved_df["SRC_DATABASE"] == sel_db]
        sc_opts = sorted(f1["SOURCE_SCHEMA"].dropna().unique().tolist())
        sel_sc  = st.selectbox("Source Schema", sc_opts, key="dbt_schema")

        f2 = f1[f1["SOURCE_SCHEMA"] == sel_sc]
        tbl_opts = sorted(f2["SOURCE_TABLE"].dropna().unique().tolist())
        sel_tbl  = st.selectbox("Source Table", tbl_opts, key="dbt_table")

        row        = f2[f2["SOURCE_TABLE"] == sel_tbl].iloc[0]
        src_system = row["SOURCE_SYSTEM"]
        src_schema = row["SOURCE_SCHEMA"]
        src_db     = row["SRC_DATABASE"]
        _src_system_badge(src_system)

    if not st.button("⚙️ Generate SQL Files", type="primary", key="dbt_gen_btn"):
        st.info("Select a source table above and click Generate.")
        return

    # Load data for the selected table
    ent_ids, ent_df, col_df, hash_df, saved_defs = \
        _load_table_data(sel_tbl, src_system, src_schema)

    if not ent_ids:
        st.error("No entities found in the approved workspace. Re-approve the model in Design Raw Vault.")
        return

    hash_lkp    = _build_hash_lkp(hash_df)
    stg_name    = f"stg_{src_system.lower()}_{sel_tbl.lower()}"
    ents_sorted = sorted(ent_df.to_dict("records"),
                         key=lambda e: (_TYPE_RANK.get(e["ENTITY_TYPE"], 3), e["ENTITY_ID"]))

    stg_sql      = _build_stg_sql(stg_name, src_system, sel_tbl, hash_lkp)
    entity_files = _build_entity_files(ents_sorted, col_df, hash_lkp, stg_name)

    # ── Render SQL files ──────────────────────────────────────────────────────
    st.success(
        f"Generated {1 + len(entity_files)} SQL file(s) for "
        f"**{src_system} / {sel_tbl}** → {len(ents_sorted)} vault entities."
    )
    st.info(
        "Use **Step 2** above to regenerate `sources.yml` and `schema.yml` "
        "once you have generated SQL files for all your source tables."
    )

    st.markdown("#### Where to put each file")
    st.markdown("""
| File | Folder in NexusDBT project |
|------|---------------------------|
| `sources.yml` *(from Step 2)* | `models/staging/` |
| `stg_*.sql` | `models/staging/` |
| `HUB_*.sql`, `LNK_*.sql`, `SAT_*.sql` | `models/raw_vault/` |
| `schema.yml` *(from Step 2)* | `models/raw_vault/` |
""")

    st.markdown("---")

    # Tabs: stg_xxx.sql | entity files...
    tab_labels = [f"{stg_name}.sql"] + list(entity_files.keys())
    tabs = st.tabs(tab_labels)

    with tabs[0]:
        _code_tab(f"{stg_name}.sql", stg_sql, lang="sql")

    for i, (fname, fsql) in enumerate(entity_files.items()):
        with tabs[1 + i]:
            _code_tab(fname, fsql, lang="sql")

    # ─────────────────────────────────────────────────────────────────────────
    # FOLDER STRUCTURE GUIDE
    # ─────────────────────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("#### Expected folder structure in NexusDBT after copy-paste")
    folder_preview = (
        "NexusDBT/\n"
        "├── dbt_project.yml           ← one-time setup\n"
        "├── packages.yml              ← one-time setup\n"
        "├── profiles.yml              ← one-time setup\n"
        "└── models/\n"
        "    ├── staging/\n"
        "    │   ├── sources.yml       ← from Step 2 (all tables)\n"
    )
    for stg_n in ([stg_name] if stg_name else []):
        folder_preview += f"    │   └── {stg_n}.sql\n"
    folder_preview += (
        "    └── raw_vault/\n"
        "        ├── schema.yml        ← from Step 2 (all entities)\n"
    )
    for fname in entity_files:
        folder_preview += f"        ├── {fname}\n"
    st.code(folder_preview, language="text")

    st.markdown("#### Run order once all files are in place")
    st.code(
        "dbt deps                          # install packages (first time only)\n"
        "dbt run --select staging.*        # build all staging views\n"
        "dbt run --select raw_vault.*      # load all vault tables\n"
        "dbt test --select raw_vault.*     # run dbt_expectations tests",
        language="bash"
    )


# ══════════════════════════════════════════════════════════════════════════════
# ROUTING
# ══════════════════════════════════════════════════════════════════════════════

if page == "Identify Source":
    page_source_tables()
elif page == "Profile and Review":
    page_profiling_review()
elif page == "Design Raw Vault":
    page_design_workbench()
elif page == "Generate Erwin":
    page_generate_model()
elif page == "Generate DBT":
    page_generate_dbt()
