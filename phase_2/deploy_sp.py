"""Deploy/replace stored procedures to Snowflake NEXUS.META."""
import snowflake.connector
import os

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

def split_statements(sql):
    """Split a SQL file into individual statements on '$$;' or ';' boundaries."""
    # Split on the Snowflake $$ end marker + semicolon
    import re
    # Handle AS $$ ... $$; blocks first
    parts = re.split(r'(\$\$\s*;)', sql)
    statements = []
    buf = ""
    for part in parts:
        if re.match(r'\$\$\s*;', part):
            buf += "$$"
            stmt = buf.strip()
            if stmt:
                statements.append(stmt)
            buf = ""
        else:
            # Within a non-$$ block, split on bare semicolons
            # but only if we're not inside a $$ block
            sub = part.split(';')
            for i, s in enumerate(sub):
                if i < len(sub) - 1:
                    full = (buf + s).strip()
                    if full:
                        statements.append(full)
                    buf = ""
                else:
                    buf = buf + s
    if buf.strip():
        statements.append(buf.strip())
    return [s for s in statements if s]

try:
    for fname in SP_FILES:
        fpath = os.path.join(PHASE2_DIR, fname)
        print(f"\nDeploying {fname}...")
        with open(fpath, 'r', encoding='utf-8') as f:
            sql = f.read()

        # Execute as a single statement (CREATE OR REPLACE PROCEDURE ... AS $$...$$)
        # Strip comment lines and find the CREATE statement
        # The whole file is one big statement ending with $$;
        # Just execute it directly
        try:
            cs.execute(sql)
            print(f"  OK — {cs.fetchone()}")
        except Exception as e:
            # Try splitting and running each statement
            stmts = [s.strip() for s in sql.split(';\n') if s.strip() and not s.strip().startswith('--')]
            for i, stmt in enumerate(stmts):
                stmt = stmt.rstrip(';').strip()
                if not stmt or stmt.startswith('--'):
                    continue
                try:
                    cs.execute(stmt)
                    print(f"  Statement {i+1}: OK")
                except Exception as e2:
                    print(f"  Statement {i+1} ERROR: {e2}")
                    raise

    print("\nAll stored procedures deployed successfully.")

except Exception as e:
    print(f"\nERROR: {e}")
    raise
finally:
    cs.close()
    conn.close()
