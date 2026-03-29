"""
Deploy NEXUS DV2.0 Streamlit app to Snowflake.
Fixes the [Errno 21] Is a directory error by uploading to stage root (no path suffix).
"""
import snowflake.connector
import os

ACCOUNT   = os.environ["SNOWFLAKE_ACCOUNT"]
USER      = os.environ["SNOWFLAKE_USER"]
PASSWORD  = os.environ["SNOWFLAKE_PASSWORD"]
WAREHOUSE = "COMPUTE_WH"
DATABASE  = "NEXUS"
ROLE      = "ACCOUNTADMIN"
SCHEMA    = "META"

STAGE_NAME      = "NEXUS_STREAMLIT_STAGE"
STREAMLIT_NAME  = "NEXUS_DV2_APP"
LOCAL_FILE      = os.path.join(os.path.dirname(__file__), "04_streamlit_app.py")

conn = snowflake.connector.connect(
    account=ACCOUNT, user=USER, password=PASSWORD,
    warehouse=WAREHOUSE, database=DATABASE, schema=SCHEMA, role=ROLE
)
cs = conn.cursor()

try:
    print("1. Creating stage (if not exists)...")
    cs.execute(f"""
        CREATE STAGE IF NOT EXISTS {DATABASE}.{SCHEMA}.{STAGE_NAME}
        DIRECTORY = (ENABLE = TRUE)
        COMMENT = 'Streamlit app files for NEXUS DV2.0'
    """)
    print("   OK")

    print("2. Removing any existing files from stage...")
    cs.execute(f"REMOVE @{DATABASE}.{SCHEMA}.{STAGE_NAME}")
    for row in cs.fetchall():
        print(f"   Removed: {row}")

    print("3. Uploading 04_streamlit_app.py to stage root...")
    # PUT to stage root — NOT @STAGE/filename (that creates a directory)
    cs.execute(
        f"PUT 'file://{LOCAL_FILE.replace(chr(92), '/')}' "
        f"@{DATABASE}.{SCHEMA}.{STAGE_NAME} "
        f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
    )
    for row in cs.fetchall():
        print(f"   {row}")

    print("4. Verifying stage contents...")
    cs.execute(f"LIST @{DATABASE}.{SCHEMA}.{STAGE_NAME}")
    files = cs.fetchall()
    for f in files:
        print(f"   {f}")
    if not files:
        raise RuntimeError("Stage is empty after PUT — upload may have failed.")

    print("5. Dropping old Streamlit app (if exists)...")
    cs.execute(f"DROP STREAMLIT IF EXISTS {DATABASE}.{SCHEMA}.{STREAMLIT_NAME}")
    print("   OK")

    print("6. Creating Streamlit app...")
    cs.execute(f"""
        CREATE STREAMLIT {DATABASE}.{SCHEMA}.{STREAMLIT_NAME}
            ROOT_LOCATION = '@{DATABASE}.{SCHEMA}.{STAGE_NAME}'
            MAIN_FILE = '/04_streamlit_app.py'
            QUERY_WAREHOUSE = '{WAREHOUSE}'
            COMMENT = 'NEXUS DV2.0 — Data Vault Automation Platform'
    """)
    print("   OK")

    print("7. Getting app URL...")
    cs.execute(f"SHOW STREAMLITS LIKE '{STREAMLIT_NAME}' IN SCHEMA {DATABASE}.{SCHEMA}")
    rows = cs.fetchall()
    col_names = [d[0] for d in cs.description]
    for row in rows:
        row_dict = dict(zip(col_names, row))
        url = row_dict.get('url_id') or row_dict.get('url') or ''
        print(f"   App name : {row_dict.get('name')}")
        print(f"   URL ID   : {url}")
        print(f"\n   Open in Snowsight: Apps > Streamlit > {STREAMLIT_NAME}")

    print("\nDeployment complete!")

except Exception as e:
    print(f"\nERROR: {e}")
    raise
finally:
    cs.close()
    conn.close()
