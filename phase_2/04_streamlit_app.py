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

# ── Page config & session ─────────────────────────────────────────────────────

st.set_page_config(page_title="NEXUS DV2.0", layout="wide")
session = get_active_session()

# ── Navigation ────────────────────────────────────────────────────────────────

st.sidebar.title("NEXUS DV2.0")
st.sidebar.caption("Data Vault Automation Platform")
st.sidebar.markdown("---")

page = st.sidebar.radio("Navigation",
    ["Source Tables", "Profiling Review", "Design Workbench ↗"], key="nav")

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
    """Return base tables in a given database.schema."""
    rows = session.sql(f"""
        SELECT TABLE_NAME
        FROM "{database}".INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema.replace("'","''")}' AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
    """).collect()
    return [r['TABLE_NAME'] for r in rows]

# ── Shared session state ──────────────────────────────────────────────────────

for k, v in {"sel_run_id": None, "sel_table": None}.items():
    if k not in st.session_state:
        st.session_state[k] = v


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 1: SOURCE TABLES
# ══════════════════════════════════════════════════════════════════════════════

def page_source_tables():
    st.title("Source Tables")

    col_runs, col_new = st.columns([2, 1])

    # ── Left: existing profiling runs ─────────────────────────────────────────

    with col_runs:
        st.subheader("Profiling Runs")

        runs_df = session.sql("""
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
            ORDER BY STARTED_AT DESC
            LIMIT 100
        """).to_pandas()

        if runs_df.empty:
            st.info("No profiling runs yet. Use the panel on the right to profile a table.")
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
                    st.experimental_rerun()

    # ── Right: trigger new profiling run ──────────────────────────────────────

    with col_new:
        st.subheader("Profile a Table")
        st.caption("Choose from any accessible database.")

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
        source_table = st.selectbox("Table", tables, key="new_table",
                                    disabled=not tables)

        st.markdown("---")
        source_system = st.text_input(
            "Source System Code *(optional)*",
            key="new_system",
            placeholder=f"e.g. ACCT_SYS  (leave blank to use database name)",
            help=(
                "A short code identifying the source system — separate from the "
                "database/schema path. Used in DV2.0 satellite naming: "
                "SAT_CUSTOMER_DETAILS__ACCT_SYS. One source system may span "
                "multiple databases. Leave blank to default to the database name."
            )
        )

        st.markdown("")
        run_btn = st.button("▶ Run Profiling", use_container_width=True,
                            disabled=not (source_db and source_schema and source_table))

        if run_btn:
            new_run_id = str(uuid.uuid4())
            effective_system = source_system.strip().upper() or source_db.upper()

            with st.spinner(f"Profiling {source_db}.{source_schema}.{source_table}…"):
                try:
                    session.call("META.SP_PROFILE_TABLE",
                                 source_schema, source_table,
                                 source_db,
                                 effective_system if source_system.strip() else None,
                                 new_run_id)

                    session.call("META.SP_DETECT_PK_CANDIDATES", new_run_id)
                    session.call("META.SP_DETECT_CHANGE_FREQUENCY", new_run_id)

                    st.session_state.sel_run_id = new_run_id
                    st.session_state.sel_table  = source_table
                    get_databases.clear()  # bust cache in case new db was added

                    st.success("Profiling complete! Switch to **Profiling Review** to see results.")
                    st.experimental_rerun()

                except Exception as e:
                    st.error(f"Profiling failed: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 2: PROFILING REVIEW
# ══════════════════════════════════════════════════════════════════════════════

def page_profiling_review():
    st.title("Profiling Review")

    runs = session.sql("""
        SELECT
            RUN_ID,
            COALESCE(SOURCE_DATABASE,'?') || '.' || SOURCE_SCHEMA || '.' || SOURCE_TABLE
                || '  [' || SOURCE_SYSTEM || ']'
                || '  (' || TO_CHAR(STARTED_AT,'YYYY-MM-DD') || ')' AS LABEL,
            SOURCE_TABLE, SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_SYSTEM
        FROM META.DV_PROFILING_RUN
        WHERE STATUS = 'COMPLETED'
        ORDER BY STARTED_AT DESC
        LIMIT 50
    """).to_pandas()

    if runs.empty:
        st.info("No completed profiling runs. Go to Source Tables and profile a table first.")
        return

    # Pre-select run from session state if available
    default_idx = 0
    if st.session_state.sel_run_id:
        match = runs[runs["RUN_ID"] == st.session_state.sel_run_id]
        if not match.empty:
            default_idx = int(match.index[0])

    selected_label = st.selectbox("Profiling run:", runs["LABEL"].tolist(), index=default_idx)
    sel_row = runs[runs["LABEL"] == selected_label].iloc[0]
    run_id  = sel_row["RUN_ID"]

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

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Database",   info["DB"])
    m2.metric("Source System", info["SOURCE_SYSTEM"])
    m3.metric("Rows",       f"{int(info['ROW_COUNT'] or 0):,}")
    m4.metric("Columns",    info["COLUMN_COUNT"])
    m5.metric("Method",     info["PROFILING_METHOD"])

    tab1, tab2 = st.tabs(["Column Statistics", "PK Candidates"])

    # ── Tab 1: Column stats ───────────────────────────────────────────────────

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
                CHANGE_FREQUENCY              AS "Change Freq",
                IFF(IS_PK_CANDIDATE,'🔑','')  AS "PK?"
            FROM META.DV_PROFILING_RESULTS
            WHERE RUN_ID = '{run_id}'
            ORDER BY ORDINAL_POSITION
        """).to_pandas()

        def colour_freq(val):
            return {"FAST":"background-color:#fff3cd",
                    "SLOW":"background-color:#d4edda",
                    "STATIC":"background-color:#cce5ff"}.get(val,"")

        st.dataframe(
            col_data.style.map(colour_freq, subset=["Change Freq"]),
            use_container_width=True
        )

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
                        import json
                        tv = d.get("TOP_VALUES")
                        vals = json.loads(str(tv)) if isinstance(tv, str) else (list(tv) if tv else [])
                        st.write("**Top Values:**")
                        for v in vals: st.write(f"  • `{v}`")
                    except Exception:
                        pass

    # ── Tab 2: PK Candidates ──────────────────────────────────────────────────

    with tab2:
        st.subheader("Primary Key Candidates")
        st.caption("Review detected candidates and confirm the Business Key.")

        pk_data = session.sql(f"""
            SELECT
                CANDIDATE_ID,
                COLUMN_NAMES::VARCHAR              AS "Columns",
                CANDIDATE_TYPE                     AS "Type",
                PK_SCORE                           AS "Score",
                IFF(PK_SCORE>=60,'✅ Strong',IFF(PK_SCORE>=40,'⚠️ Possible','❌ Weak')) AS "Strength",
                IFF(MODELER_SELECTED,'✔ Confirmed','') AS "Confirmed"
            FROM META.DV_PK_CANDIDATES
            WHERE RUN_ID = '{run_id}'
            ORDER BY PK_SCORE DESC
        """).to_pandas()

        if pk_data.empty:
            st.warning("No PK candidates found. The table may not have a uniquely identifying column.")
        else:
            st.dataframe(pk_data[["Columns","Type","Score","Strength","Confirmed"]],
                         use_container_width=True)

            selected_bk = st.selectbox("Confirm Business Key:", pk_data["Columns"].tolist(),
                                       key="bk_sel")

            if st.button("✔ Confirm as Business Key"):
                cand_id = int(pk_data[pk_data["Columns"] == selected_bk]["CANDIDATE_ID"].values[0])
                session.sql(f"""
                    UPDATE META.DV_PK_CANDIDATES
                    SET MODELER_SELECTED=FALSE, SELECTED_BY=NULL, SELECTED_DATE=NULL
                    WHERE RUN_ID='{run_id}'
                """).collect()
                session.sql(f"""
                    UPDATE META.DV_PK_CANDIDATES
                    SET MODELER_SELECTED=TRUE,
                        SELECTED_BY=CURRENT_USER(),
                        SELECTED_DATE=CURRENT_TIMESTAMP()
                    WHERE CANDIDATE_ID={cand_id}
                """).collect()
                session.sql(f"""
                    INSERT INTO META.DV_AUDIT_LOG
                        (ACTION_TYPE,ENTITY_TYPE,ENTITY_ID,SOURCE_TABLE,SOURCE_SYSTEM,ACTION_DETAILS)
                    SELECT 'PK_CONFIRM','RUN','{run_id}',
                           '{info["SOURCE_TABLE"].replace("'","''")}',
                           '{info["SOURCE_SYSTEM"].replace("'","''")}',
                           PARSE_JSON('{{"run_id":"{run_id}","confirmed_bk":"{selected_bk}"}}')
                """).collect()
                st.success(f"Business Key confirmed: **{selected_bk}**")
                st.experimental_rerun()

        # ── Modeler PK override ───────────────────────────────────────────────
        st.markdown("---")
        st.markdown("**Modeler PK Override**")
        st.caption("If the detected candidates are wrong, enter your own primary key here. "
                   "For composite keys use comma-separated column names, e.g. `POLICY_ID, LINE_NBR`")
        pk_override = st.text_input(
            "Primary key column(s):", key="pk_override_input",
            placeholder="CUSTOMER_ID  or  POLICY_ID, LINE_NBR"
        )
        if st.button("💾 Save PK Override", key="pk_override_btn"):
            if pk_override.strip():
                cols_clean = ", ".join(
                    [c.strip().upper() for c in pk_override.split(",") if c.strip()]
                )
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
                    UPDATE META.DV_PK_CANDIDATES
                    SET MODELER_SELECTED=FALSE
                    WHERE RUN_ID='{run_id}' AND CANDIDATE_TYPE != 'MODELER_OVERRIDE'
                """).collect()
                st.success(f"PK override saved: **{cols_clean}**")
                st.experimental_rerun()
            else:
                st.warning("Enter at least one column name.")

    # ── Change frequency chart ────────────────────────────────────────────────

    st.markdown("---")
    st.subheader("Change Frequency Distribution")
    freq_df = session.sql(f"""
        SELECT COALESCE(CHANGE_FREQUENCY,'UNKNOWN') AS FREQ, COUNT(*) AS CNT
        FROM META.DV_PROFILING_RESULTS WHERE RUN_ID='{run_id}'
        GROUP BY CHANGE_FREQUENCY ORDER BY CNT DESC
    """).to_pandas()
    if not freq_df.empty:
        freq_df.columns = ["Change Frequency","Column Count"]
        st.bar_chart(freq_df.set_index("Change Frequency"))


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3: DESIGN WORKBENCH
# ══════════════════════════════════════════════════════════════════════════════

# ── Confidence badge colours ──────────────────────────────────────────────────
CONF_COLOUR = {"HIGH": "🟢", "MEDIUM": "🟡", "LOW": "🔴", "INFERRED": "⚪"}
ENTITY_TYPE_COLOUR = {"HUB": "🔵", "LNK": "🟢", "SAT": "🟠", "MSAT": "🟣", "ESAT": "🔴"}

def _ws_key(*parts):
    """Generate a unique session-state key from parts."""
    return "__wb_" + "_".join(str(p) for p in parts)

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
                        '{col.get("source_column","").replace("'","''")}' AS SOURCE_COLUMN
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


def _render_entity_card(ws: dict, entity_list_key: str, idx: int) -> dict:
    """Render one entity card with editable fields. Returns modified entity dict."""
    ent      = ws[entity_list_key][idx]
    eid      = ent.get('entity_id', f'ENTITY_{idx}')
    etype    = ent.get('entity_type', entity_list_key[:3].upper())
    conf     = ent.get('confidence', 'MEDIUM')
    status   = ent.get('entity_status', 'NEW')
    is_new   = ent.get('is_new', True)

    border_colour = {"NEW": "#28a745", "EXISTING": "#0066cc", "MODIFIED": "#fd7e14"}.get(status, "#999")
    type_icon = ENTITY_TYPE_COLOUR.get(etype, "⬜")
    conf_icon = CONF_COLOUR.get(conf, "⚪")

    label = f"{type_icon} {eid}  {conf_icon} {conf}"
    if not is_new:
        label += "  *(reused from registry)*"

    with st.expander(label, expanded=(status == 'NEW')):
        c1, c2, c3 = st.columns([2, 2, 1])
        new_eid = c1.text_input("Entity Name", value=eid,
                                key=_ws_key(entity_list_key, idx, 'eid'))
        new_logical = c2.text_input("Logical Name", value=ent.get('logical_name',''),
                                    key=_ws_key(entity_list_key, idx, 'logical'))
        domain_opts = ['', 'PARTY', 'ACCOUNT', 'FINANCE', 'PRODUCT', 'REFERENCE', 'TRANSACTION', 'OTHER']
        cur_domain = ent.get('domain','') or ''
        domain_idx = domain_opts.index(cur_domain) if cur_domain in domain_opts else 0
        new_domain = c3.selectbox("Domain", domain_opts, index=domain_idx,
                                  key=_ws_key(entity_list_key, idx, 'domain'))

        if ent.get('rationale'):
            st.caption(f"AI rationale: {ent['rationale']}")

        if ent.get('parent_entity_id'):
            st.caption(f"Parent: {ent['parent_entity_id']}")

        # ── Columns table ─────────────────────────────────────────────────────
        st.markdown("**Columns**")
        cols = ent.get('columns', [])
        role_opts = ['HK','BK','FK_HK','HASHDIFF','META','ATTR','MAK']
        del_flags = []

        hdr = st.columns([2, 2, 1, 2, 0.4])
        hdr[0].markdown("**Column Name**")
        hdr[1].markdown("**Data Type**")
        hdr[2].markdown("**Role**")
        hdr[3].markdown("**Definition**")
        hdr[4].markdown("**Del**")

        new_cols = []
        for ci, col in enumerate(cols):
            cc = st.columns([2, 2, 1, 2, 0.4])
            col_name = cc[0].text_input("", value=col.get('column_name',''),
                                        label_visibility="collapsed",
                                        key=_ws_key(entity_list_key, idx, 'col', ci, 'name'))
            col_type = cc[1].text_input("", value=col.get('data_type','VARCHAR'),
                                        label_visibility="collapsed",
                                        key=_ws_key(entity_list_key, idx, 'col', ci, 'type'))
            role_val = col.get('column_role', 'ATTR')
            role_idx = role_opts.index(role_val) if role_val in role_opts else len(role_opts)-1
            col_role = cc[2].selectbox("", role_opts, index=role_idx,
                                       label_visibility="collapsed",
                                       key=_ws_key(entity_list_key, idx, 'col', ci, 'role'))
            col_defn = cc[3].text_input("", value=col.get('column_definition',''),
                                        label_visibility="collapsed",
                                        key=_ws_key(entity_list_key, idx, 'col', ci, 'defn'))
            delete_col = cc[4].checkbox("", value=False,
                                        key=_ws_key(entity_list_key, idx, 'col', ci, 'del'),
                                        label_visibility="collapsed")
            if not delete_col:
                new_cols.append({**col,
                    'column_name': col_name, 'data_type': col_type,
                    'column_role': col_role, 'column_definition': col_defn})

        if st.button("＋ Add Column", key=_ws_key(entity_list_key, idx, 'add_col')):
            new_cols.append({'column_name': 'NEW_COLUMN', 'data_type': 'VARCHAR',
                             'column_role': 'ATTR', 'column_definition': ''})

        # Determine status
        updated_status = status
        if new_eid != eid or new_logical != ent.get('logical_name','') or new_cols != cols:
            updated_status = 'MODIFIED' if status == 'EXISTING' else status

        return {**ent,
            'entity_id':     new_eid,
            'logical_name':  new_logical,
            'domain':        new_domain,
            'columns':       new_cols,
            'entity_status': updated_status}


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
        'wb_workspace_id': None,
        'wb_workspace':    None,
        'wb_source_key':   None,
    }.items():
        if k not in st.session_state:
            st.session_state[k] = v

    st.title("Design Workbench")

    # ── Load source list ──────────────────────────────────────────────────────
    sources_df = session.sql("""
        SELECT
            w.WORKSPACE_ID,
            w.SOURCE_TABLE,
            w.SOURCE_SYSTEM,
            w.SOURCE_SCHEMA,
            w.STATUS                                    AS WS_STATUS,
            w.AI_CONFIDENCE,
            w.INPUT_SCENARIO,
            TO_CHAR(w.LAST_MODIFIED,'YYYY-MM-DD HH24:MI') AS LAST_MOD,
            w.LAST_MODIFIED_BY,
            r.RUN_ID IS NOT NULL                        AS HAS_PROFILING
        FROM META.DV_DESIGN_WORKSPACE w
        LEFT JOIN META.DV_PROFILING_RUN r
               ON r.SOURCE_TABLE  = w.SOURCE_TABLE
              AND r.SOURCE_SYSTEM = w.SOURCE_SYSTEM
              AND r.STATUS        = 'COMPLETED'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY w.SOURCE_TABLE, w.SOURCE_SYSTEM
            ORDER BY w.LAST_MODIFIED DESC
        ) = 1
        ORDER BY w.LAST_MODIFIED DESC
    """).to_pandas()

    # Also get profiled tables without a workspace yet
    profiled_df = session.sql("""
        SELECT SOURCE_TABLE, SOURCE_SYSTEM, SRC_DB, SOURCE_SCHEMA, LAST_PROFILED
        FROM (
            SELECT
                r.SOURCE_TABLE,
                r.SOURCE_SYSTEM,
                COALESCE(r.SOURCE_DATABASE,'?') AS SRC_DB,
                r.SOURCE_SCHEMA,
                TO_CHAR(MAX(r.COMPLETED_AT),'YYYY-MM-DD') AS LAST_PROFILED,
                MAX(r.COMPLETED_AT) AS MAX_DT
            FROM META.DV_PROFILING_RUN r
            LEFT JOIN META.DV_DESIGN_WORKSPACE w
                   ON w.SOURCE_TABLE  = r.SOURCE_TABLE
                  AND w.SOURCE_SYSTEM = r.SOURCE_SYSTEM
            WHERE r.STATUS = 'COMPLETED'
              AND w.WORKSPACE_ID IS NULL
            GROUP BY r.SOURCE_TABLE, r.SOURCE_SYSTEM, r.SOURCE_DATABASE, r.SOURCE_SCHEMA
        )
        ORDER BY MAX_DT DESC
    """).to_pandas()

    col_explorer, col_main = st.columns([1, 3])

    # ══ LEFT PANEL: SOURCE EXPLORER ═══════════════════════════════════════════
    with col_explorer:
        st.subheader("Source Explorer")

        status_icon = {'DRAFT': '◑', 'IN_REVIEW': '🔍', 'APPROVED': '✅', 'SUPERSEDED': '⬜'}

        if not sources_df.empty:
            st.caption("Workspaces")
            for _, row in sources_df.iterrows():
                icon = status_icon.get(row['WS_STATUS'], '◑')
                label = f"{icon} {row['SOURCE_TABLE']}  [{row['SOURCE_SYSTEM']}]"
                if st.button(label, key=f"ws_btn_{row['WORKSPACE_ID']}", use_container_width=True):
                    st.session_state.wb_workspace_id = row['WORKSPACE_ID']
                    st.session_state.wb_workspace    = None   # force reload
                    st.session_state.wb_source_key   = f"{row['SOURCE_SYSTEM']}__{row['SOURCE_TABLE']}"
                    st.experimental_rerun()

        if not profiled_df.empty:
            st.caption("Profiled — no workspace yet")
            for _, row in profiled_df.iterrows():
                label = f"● {row['SOURCE_TABLE']}  [{row['SOURCE_SYSTEM']}]"
                if st.button(label, key=f"prof_btn_{row['SOURCE_TABLE']}_{row['SOURCE_SYSTEM']}",
                             use_container_width=True):
                    st.session_state.wb_workspace_id = None
                    st.session_state.wb_workspace    = None
                    st.session_state.wb_source_key   = f"{row['SOURCE_SYSTEM']}__{row['SOURCE_TABLE']}"
                    st.experimental_rerun()

        st.markdown("---")
        st.caption("Generate from profiled table:")
        gen_sources = []
        if not profiled_df.empty:
            gen_sources += [(f"{r['SOURCE_TABLE']} [{r['SOURCE_SYSTEM']}]",
                             r['SOURCE_TABLE'], r['SOURCE_SYSTEM'], r['SOURCE_SCHEMA'])
                            for _, r in profiled_df.iterrows()]
        if not sources_df.empty:
            for _, r in sources_df.iterrows():
                gen_sources.append((
                    f"{r['SOURCE_TABLE']} [{r['SOURCE_SYSTEM']}] ↻",
                    r['SOURCE_TABLE'], r['SOURCE_SYSTEM'], r['SOURCE_SCHEMA']
                ))

        if gen_sources:
            gen_labels = [g[0] for g in gen_sources]
            gen_sel = st.selectbox("Table:", gen_labels, key="wb_gen_sel",
                                   label_visibility="collapsed")

            st.markdown("**Modeler Notes** *(optional but recommended)*")
            st.caption(
                "Tell the AI what you know about this table. Include:\n"
                "- Table purpose and business context\n"
                "- Primary key if you know it\n"
                "- Column definitions missing from the source system\n"
                "- Relationships to other tables\n"
                "- Columns to ignore or treat specially"
            )
            modeler_notes = st.text_area(
                "Notes:", key="wb_modeler_notes", height=150,
                placeholder=(
                    "Example: This is the master account table from core banking. "
                    "ACCT_ID is always the primary key. STATUS_CD changes frequently. "
                    "OPEN_DT is static after account creation. "
                    "LEGACY_REF column is deprecated — ignore it. "
                    "This table links to ACCT_TRANS via ACCT_ID."
                )
            )

            if st.button("🤖 Generate AI Proposal", use_container_width=True, key="wb_gen_btn"):
                chosen = next((g for g in gen_sources if g[0] == gen_sel), None)
                if chosen:
                    _, tbl, sys_, sch_ = chosen
                    with st.spinner("Calling Cortex AI… this may take 30–60 seconds"):
                        try:
                            ws_id = session.call(
                                "META.SP_GENERATE_DV_PROPOSAL",
                                tbl, sys_, sch_, None, None,
                                modeler_notes.strip() if modeler_notes.strip() else None
                            )
                            st.session_state.wb_workspace_id = ws_id
                            st.session_state.wb_workspace    = None
                            st.session_state.wb_source_key   = f"{sys_}__{tbl}"
                            st.experimental_rerun()
                        except Exception as e:
                            st.error(f"Generation failed: {e}")
        else:
            st.info("Profile a table first to generate an AI proposal.")

    # ══ RIGHT PANEL: MAIN WORK AREA ════════════════════════════════════════════
    with col_main:

        # ── No selection ─────────────────────────────────────────────────────
        if not st.session_state.wb_workspace_id and not st.session_state.wb_source_key:
            st.info("Select a source from the explorer, or generate a new AI proposal.")
            return

        # ── Load workspace if needed ─────────────────────────────────────────
        ws_id = st.session_state.wb_workspace_id
        if ws_id and st.session_state.wb_workspace is None:
            st.session_state.wb_workspace = _load_workspace(ws_id)
        ws = st.session_state.wb_workspace or {}

        meta   = ws.get('_meta', {})
        src_tbl = meta.get('source_table') or (st.session_state.wb_source_key or '').split('__')[-1]
        src_sys = meta.get('source_system') or (st.session_state.wb_source_key or '').split('__')[0]

        # ── Header ───────────────────────────────────────────────────────────
        h1, h2, h3 = st.columns([2, 1, 1])
        h1.markdown(f"### {src_tbl}  `[{src_sys}]`")
        if ws_id:
            status_row = session.sql(f"""
                SELECT STATUS, AI_CONFIDENCE, INPUT_SCENARIO
                FROM META.DV_DESIGN_WORKSPACE
                WHERE WORKSPACE_ID = '{ws_id.replace("'","''")}'
            """).to_pandas()
            if not status_row.empty:
                r = status_row.iloc[0]
                h2.metric("Status", r['STATUS'])
                h3.metric("AI Confidence",
                          f"{CONF_COLOUR.get(r['AI_CONFIDENCE'],'')} {r['AI_CONFIDENCE'] or '?'}")

        if ws.get('warnings'):
            for w_msg in ws['warnings']:
                st.warning(w_msg)

        tab_meta, tab_model, tab_diagram = st.tabs(
            ["📋 Metadata / Profiling", "✏️ DV Model", "📊 Diagram"])

        # ── TAB 1: Metadata / Profiling ───────────────────────────────────────
        with tab_meta:
            run_id_meta = meta.get('run_id')
            if not run_id_meta:
                # Find latest run for this table/system
                lr = session.sql(f"""
                    SELECT RUN_ID FROM META.DV_PROFILING_RUN
                    WHERE SOURCE_TABLE  = '{src_tbl.replace("'","''")}'
                      AND SOURCE_SYSTEM = '{src_sys.replace("'","''")}'
                      AND STATUS = 'COMPLETED'
                    ORDER BY COMPLETED_AT DESC LIMIT 1
                """).collect()
                run_id_meta = lr[0]['RUN_ID'] if lr else None

            if run_id_meta:
                run_info = session.sql(f"""
                    SELECT COALESCE(SOURCE_DATABASE,'?') AS DB,
                           SOURCE_SCHEMA, SOURCE_TABLE, ROW_COUNT,
                           COLUMN_COUNT, PROFILING_METHOD,
                           TO_CHAR(COMPLETED_AT,'YYYY-MM-DD HH24:MI') AS COMPLETED
                    FROM META.DV_PROFILING_RUN
                    WHERE RUN_ID = '{run_id_meta.replace("'","''")}'
                """).to_pandas()
                if not run_info.empty:
                    ri = run_info.iloc[0]
                    m1,m2,m3,m4 = st.columns(4)
                    m1.metric("Rows",    f"{int(ri['ROW_COUNT'] or 0):,}")
                    m2.metric("Columns", ri['COLUMN_COUNT'])
                    m3.metric("Method",  ri['PROFILING_METHOD'])
                    m4.metric("Profiled",ri['COMPLETED'])

                prof_data = session.sql(f"""
                    SELECT ORDINAL_POSITION AS "#", COLUMN_NAME,
                           SOURCE_DATA_TYPE AS "Src Type",
                           INFERRED_DATA_TYPE AS "Inferred",
                           PATTERN_DETECTED AS "Pattern",
                           ROUND(UNIQUENESS_RATIO*100,2) AS "Uniq%",
                           ROUND(NULL_PERCENTAGE,2) AS "Null%",
                           CHANGE_FREQUENCY AS "Chg Freq",
                           IFF(IS_PK_CANDIDATE,'🔑','') AS "PK?"
                    FROM META.DV_PROFILING_RESULTS
                    WHERE RUN_ID = '{run_id_meta.replace("'","''")}'
                    ORDER BY ORDINAL_POSITION
                """).to_pandas()
                st.dataframe(prof_data, use_container_width=True)

                pk_data = session.sql(f"""
                    SELECT COLUMN_NAMES::VARCHAR AS "Business Key",
                           CANDIDATE_TYPE AS "Type", PK_SCORE AS "Score",
                           IFF(MODELER_SELECTED, '✔ Confirmed','') AS "Confirmed"
                    FROM META.DV_PK_CANDIDATES
                    WHERE RUN_ID = '{run_id_meta.replace("'","''")}'
                    ORDER BY PK_SCORE DESC
                """).to_pandas()
                if not pk_data.empty:
                    st.subheader("PK Candidates")
                    st.dataframe(pk_data, use_container_width=True)
            else:
                st.info("No profiling data available for this table.")
                scenario = meta.get('input_scenario','')
                if scenario:
                    st.caption(f"Input scenario used for AI: **{scenario}**")

        # ── TAB 2: DV Model ───────────────────────────────────────────────────
        with tab_model:
            if not ws:
                st.info("No workspace loaded. Generate an AI proposal from the left panel.")
            else:
                total = (len(ws.get('hubs',[])) +
                         len(ws.get('links',[])) +
                         len(ws.get('satellites',[])))
                st.caption(f"{total} entities — edit names, types, columns inline. Save when done.")

                # Render all entities in place — collect updated versions
                new_ws = dict(ws)

                if ws.get('hubs'):
                    st.markdown("#### Hubs")
                    new_hubs = []
                    for i, ent in enumerate(ws['hubs']):
                        ent_copy = dict(ent)
                        ent_copy['entity_type'] = 'HUB'
                        ws['hubs'][i] = ent_copy
                        updated = _render_entity_card(ws, 'hubs', i)
                        new_hubs.append(updated)
                    new_ws['hubs'] = new_hubs

                if ws.get('links'):
                    st.markdown("#### Links")
                    new_links = []
                    for i, ent in enumerate(ws['links']):
                        ent_copy = dict(ent)
                        ent_copy['entity_type'] = 'LNK'
                        ws['links'][i] = ent_copy
                        updated = _render_entity_card(ws, 'links', i)
                        new_links.append(updated)
                    new_ws['links'] = new_links

                if ws.get('satellites'):
                    st.markdown("#### Satellites")
                    new_sats = []
                    for i, ent in enumerate(ws['satellites']):
                        ent_copy = dict(ent)
                        if 'entity_type' not in ent_copy:
                            ent_copy['entity_type'] = ent_copy.get('satellite_type','SAT')
                        ws['satellites'][i] = ent_copy
                        updated = _render_entity_card(ws, 'satellites', i)
                        new_sats.append(updated)
                    new_ws['satellites'] = new_sats

                # ── Add entity ────────────────────────────────────────────────
                st.markdown("---")
                with st.expander("＋ Add Entity manually"):
                    ac1, ac2, ac3 = st.columns(3)
                    new_ename  = ac1.text_input("Entity Name", key="wb_add_name", placeholder="HUB_ACCOUNT")
                    new_etype  = ac2.selectbox("Type", ['HUB','LNK','SAT','MSAT','ESAT'], key="wb_add_type")
                    new_elogic = ac3.text_input("Logical Name", key="wb_add_logical")
                    if st.button("Add Entity", key="wb_add_btn"):
                        new_entity = {
                            'entity_id':     new_ename,
                            'entity_type':   new_etype,
                            'logical_name':  new_elogic,
                            'domain':        '',
                            'is_new':        True,
                            'confidence':    'LOW',
                            'entity_status': 'NEW',
                            'rationale':     'Manually added by modeler.',
                            'columns': [
                                {'column_name': 'LOAD_DTS','data_type': 'TIMESTAMP_NTZ',
                                 'column_role': 'META','column_definition': 'Load timestamp'},
                                {'column_name': 'REC_SRC','data_type': 'VARCHAR(100)',
                                 'column_role': 'META','column_definition': 'Record source'},
                            ]
                        }
                        target = 'hubs' if new_etype == 'HUB' else ('links' if new_etype == 'LNK' else 'satellites')
                        new_ws.setdefault(target, []).append(new_entity)
                        st.session_state.wb_workspace = new_ws
                        st.experimental_rerun()

                # ── Action buttons ────────────────────────────────────────────
                st.markdown("---")
                btn1, btn2, btn3 = st.columns(3)

                if btn1.button("💾 Save", use_container_width=True, key="wb_save"):
                    if ws_id:
                        st.session_state.wb_workspace = new_ws
                        _save_workspace(ws_id, new_ws)
                        st.success("Workspace saved.")
                    else:
                        st.error("No workspace ID — generate an AI proposal first.")

                if btn2.button("🔄 Re-generate", use_container_width=True, key="wb_regen"):
                    st.warning("Re-generating will replace current edits. Press again to confirm.",
                               icon="⚠️")
                    if st.button("Confirm Re-generate", key="wb_regen_confirm"):
                        with st.spinner("Calling Cortex AI…"):
                            try:
                                new_ws_id = session.call(
                                    "META.SP_GENERATE_DV_PROPOSAL",
                                    src_tbl, src_sys,
                                    meta.get('source_schema'), None, None
                                )
                                if ws_id:
                                    session.sql(f"""
                                        UPDATE META.DV_DESIGN_WORKSPACE
                                        SET STATUS = 'SUPERSEDED'
                                        WHERE WORKSPACE_ID = '{ws_id.replace("'","''")}'
                                    """).collect()
                                st.session_state.wb_workspace_id = new_ws_id
                                st.session_state.wb_workspace    = None
                                st.experimental_rerun()
                            except Exception as e:
                                st.error(f"Re-generation failed: {e}")

                if btn3.button("✅ Approve All", use_container_width=True, key="wb_approve"):
                    if ws_id:
                        # Save latest edits first
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

        # ── TAB 3: Diagram ────────────────────────────────────────────────────
        with tab_diagram:
            if not ws:
                st.info("Generate an AI proposal to see the diagram.")
            else:
                if st.button("📊 Generate Diagram", key="wb_diagram_btn"):
                    dot_code = _generate_dot(ws)
                    try:
                        st.graphviz_chart(dot_code)
                    except Exception as dg_err:
                        st.warning(f"Graphviz render failed: {dg_err}")
                    with st.expander("DOT source"):
                        st.code(dot_code, language="text")


# ══════════════════════════════════════════════════════════════════════════════
# ROUTING
# ══════════════════════════════════════════════════════════════════════════════

if page == "Source Tables":
    page_source_tables()
elif page == "Profiling Review":
    page_profiling_review()
elif page == "Design Workbench ↗":
    page_design_workbench()
