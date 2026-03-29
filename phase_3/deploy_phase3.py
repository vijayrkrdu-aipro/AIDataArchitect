"""Deploy Phase 3: SP_GENERATE_DV_PROPOSAL + updated Streamlit app."""
import snowflake.connector
import os

ACCOUNT   = os.environ["SNOWFLAKE_ACCOUNT"]
USER      = os.environ["SNOWFLAKE_USER"]
PASSWORD  = os.environ["SNOWFLAKE_PASSWORD"]
WAREHOUSE = "COMPUTE_WH"
DATABASE  = "NEXUS"
ROLE      = "ACCOUNTADMIN"
SCHEMA    = "META"

PHASE3_DIR = os.path.dirname(__file__)
PHASE2_DIR = os.path.join(PHASE3_DIR, '..', 'phase_2')
APP_FILE   = os.path.join(PHASE2_DIR, '04_streamlit_app.py')
STAGE_NAME = "NEXUS_STREAMLIT_STAGE"
APP_NAME   = "NEXUS_DV2_APP"

conn = snowflake.connector.connect(
    account=ACCOUNT, user=USER, password=PASSWORD,
    warehouse=WAREHOUSE, database=DATABASE, schema=SCHEMA, role=ROLE
)
cs = conn.cursor()

try:
    # ── 1. Deploy SP_GENERATE_DV_PROPOSAL ────────────────────────────────────
    print("1. Deploying SP_GENERATE_DV_PROPOSAL...")
    sp_file = os.path.join(PHASE3_DIR, '01_sp_generate_dv_proposal.sql')
    with open(sp_file, 'r', encoding='utf-8') as f:
        sp_sql = f.read()
    for cur in conn.execute_string(sp_sql, remove_comments=True):
        pass   # consume all result sets
    print("   OK")

    # ── 2. Re-upload Streamlit app (now includes Phase 3 workbench) ───────────
    print("2. Uploading updated Streamlit app...")
    cs.execute(f"REMOVE @{DATABASE}.{SCHEMA}.{STAGE_NAME}")
    for row in cs.fetchall():
        print(f"   Removed: {row[0]}")

    cs.execute(
        f"PUT 'file://{APP_FILE.replace(chr(92), '/')}' "
        f"@{DATABASE}.{SCHEMA}.{STAGE_NAME} "
        f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
    )
    for row in cs.fetchall():
        print(f"   {row}")

    # ── 3. Recreate Streamlit app ─────────────────────────────────────────────
    print("3. Recreating Streamlit app...")
    cs.execute(f"DROP STREAMLIT IF EXISTS {DATABASE}.{SCHEMA}.{APP_NAME}")
    cs.execute(f"""
        CREATE STREAMLIT {DATABASE}.{SCHEMA}.{APP_NAME}
            ROOT_LOCATION = '@{DATABASE}.{SCHEMA}.{STAGE_NAME}'
            MAIN_FILE     = '/04_streamlit_app.py'
            QUERY_WAREHOUSE = 'COMPUTE_WH'
            COMMENT = 'NEXUS DV2.0 — Data Vault Automation Platform (Phases 2 & 3)'
    """)
    print("   OK")

    # ── 4. Verify ─────────────────────────────────────────────────────────────
    cs.execute(f"SHOW STREAMLITS LIKE '{APP_NAME}' IN SCHEMA {DATABASE}.{SCHEMA}")
    rows = cs.fetchall()
    col_names = [d[0] for d in cs.description]
    for row in rows:
        d = dict(zip(col_names, row))
        print(f"\n   App: {d.get('name')}  URL ID: {d.get('url_id')}")
        print(f"   Open in Snowsight: Apps > Streamlit > {APP_NAME}")

    print("\nPhase 3 deployment complete.")

except Exception as e:
    print(f"\nERROR: {e}")
    raise
finally:
    cs.close()
    conn.close()
