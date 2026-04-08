"""Deploy/replace stored procedures to Snowflake NEXUS.META."""
import snowflake.connector
import os

# Load .env from the project root (two levels up from this file)
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
except ImportError:
    pass  # dotenv not installed — fall back to environment variables

ACCOUNT   = os.environ["SNOWFLAKE_ACCOUNT"]
USER      = os.environ["SNOWFLAKE_USER"]
PASSWORD  = os.environ["SNOWFLAKE_PASSWORD"]
WAREHOUSE = "COMPUTE_WH"
DATABASE  = "NEXUS"
ROLE      = "ACCOUNTADMIN"
SCHEMA    = "META"

PHASE2_DIR = os.path.dirname(__file__)

# SP files to deploy (in order)
SP_FILES = [
    "01_sp_profile_table.sql",
    "02_sp_detect_pk_candidates.sql",
    "03_sp_detect_change_frequency.sql",
]

conn = snowflake.connector.connect(
    account=ACCOUNT, user=USER, password=PASSWORD,
    warehouse=WAREHOUSE, database=DATABASE, schema=SCHEMA, role=ROLE
)
cs = conn.cursor()

def run_file(connection, fpath):
    """Execute every statement in a SQL file using execute_string (handles $$ blocks)."""
    with open(fpath, 'r', encoding='utf-8') as f:
        sql = f.read()
    for result in connection.execute_string(sql, remove_comments=True):
        pass  # consume all result sets

try:
    for fname in SP_FILES:
        fpath = os.path.join(PHASE2_DIR, fname)
        print(f"\nDeploying {fname}...")
        run_file(conn, fpath)
        print(f"  OK")

    print("\nAll stored procedures deployed successfully.")

except Exception as e:
    print(f"\nERROR: {e}")
    raise
finally:
    cs.close()
    conn.close()
