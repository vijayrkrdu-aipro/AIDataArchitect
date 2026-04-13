"""
Update META.DV_AI_SYSTEM_PROMPT and META.DV_ABBREVIATION with:
 - Revised naming conventions (satellite/link/MSAT/ESAT)
 - BATCH_ID added to all metadata column standards
 - Satellite simplification guardrails
 - Source system abbreviations
"""
import snowflake.connector
import os

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
except ImportError:
    pass

ACCOUNT   = os.environ["SNOWFLAKE_ACCOUNT"]
USER      = os.environ["SNOWFLAKE_USER"]
PASSWORD  = os.environ["SNOWFLAKE_PASSWORD"]
WAREHOUSE = "COMPUTE_WH"
DATABASE  = "NEXUS"
ROLE      = "ACCOUNTADMIN"
SCHEMA    = "META"

# ── Section content ──────────────────────────────────────────────────────────

NAMING_CONVENTIONS = """\
## NAMING CONVENTIONS

### Entity Naming

| Entity Type      | Pattern                                           | Example                        |
|------------------|---------------------------------------------------|--------------------------------|
| Hub              | HUB_<NOUN>                                        | HUB_CUSTOMER                   |
| Link             | LNK_<NOUN1>_<NOUN2> (alphabetical, abbreviated)   | LNK_ACCT_CUST                  |
| Satellite        | SAT_<PARENT_NOUN>_<DESCRIPTOR>__<SRC>             | SAT_CUSTOMER_DTL__ACCTS        |
| Multi-Active Sat | MSAT_<PARENT_NOUN>_<DESCRIPTOR>__<SRC>            | MSAT_CUSTOMER_PHONE__CRM       |
| Effectivity Sat  | ESAT_<LINK_NOUN>_<DESCRIPTOR>__<SRC>              | ESAT_ACCT_CUST__ACCTS          |
| PIT Table        | PIT_<HUB_NOUN>                                    | PIT_CUSTOMER                   |
| Bridge Table     | BRG_<CONCEPT>                                     | BRG_CUST_ACCT                  |

### Source System Code Rules (<SRC> suffix)
- Maximum 5 uppercase alphanumeric characters — NO underscores inside the suffix
- Derived from the source system abbreviation table (DOMAIN = 'SOURCE_SYSTEM')
- Always look up the abbreviation table before assigning a source code
- Examples: ACCT_SYS -> ACCTS | CORE_BANKING -> CBNK | CRM_SYS -> CRM | MDM -> MDM

### Satellite Descriptor Rules
- Maximum 2 abbreviated words from the abbreviation table
- Prefer a single word where possible
- Standard descriptors (use these before inventing new ones):

| Meaning                    | Descriptor |
|----------------------------|------------|
| Details / general attrs    | DTL        |
| Address                    | ADDR       |
| Contact info               | CNTCT      |
| Balance / financials       | BAL        |
| Status / lifecycle         | STS        |
| Demographics               | DEMOG      |
| Identity / identifiers     | IDNT       |
| Dates / lifecycle dates    | DTS        |
| Classification / category  | CLASS      |
| Market / pricing           | MKT        |
| Reference data             | REF        |

- If a hub has only ONE satellite: always use DTL as descriptor
- If a hub has TWO satellites split by change frequency: use DTL (slow) and STS (fast)

### Name Length Guardrails
| Entity         | Target Max | Hard Max |
|----------------|------------|----------|
| Hub, Link      | 30 chars   | 40 chars |
| SAT/MSAT/ESAT  | 40 chars   | 50 chars |

- If a generated name exceeds the hard max: abbreviate the PARENT_NOUN further using the abbreviation table
- Log any abbreviation decision in simplification_notes

### General Naming Rules
- UPPER_SNAKE_CASE for all physical names
- Use abbreviations from the abbreviation table — never spell out a word that has an entry
- Business nouns are SINGULAR: HUB_CUSTOMER not HUB_CUSTOMERS
- Link nouns are in ALPHABETICAL ORDER on the abbreviated forms
- MSAT_ and ESAT_ prefixes are MANDATORY for those types — never use SAT_ for multi-active or effectivity satellites

### Column Naming

| Column Role      | Pattern                      | Data Type      | Example                  |
|------------------|------------------------------|----------------|--------------------------|
| Hash Key         | <NOUN>_HK                    | BINARY(32)     | CUSTOMER_HK              |
| Business Key     | Abbreviated form             | Source type    | CUST_ID, ACCT_NBR        |
| Hashdiff         | <ENTITY_SHORT>_HASHDIFF      | BINARY(32)     | SAT_CUST_DTL_HASHDIFF    |
| Load Timestamp   | LOAD_DTS                     | TIMESTAMP_NTZ  | LOAD_DTS                 |
| Record Source    | REC_SRC                      | VARCHAR(50)    | REC_SRC                  |
| Batch ID         | BATCH_ID                     | VARCHAR(100)   | BATCH_ID                 |
| Effectivity From | EFF_FROM_DTS                 | TIMESTAMP_NTZ  | EFF_FROM_DTS             |
| Effectivity To   | EFF_TO_DTS                   | TIMESTAMP_NTZ  | EFF_TO_DTS               |
| Multi-Active Key | Abbreviated descriptor       | Source type    | PHNE_TYP_CD              |
| Attribute        | Abbreviated descriptor       | Source type    | FRST_NM, EMAIL_ADDR      |

- Column names use UPPER_SNAKE_CASE
- Apply UPPER(TRIM()) before hashing character columns
- Hashdiff column name = entity_id shortened to <=25 chars + _HASHDIFF\
"""

METADATA_COLUMNS = """\
## STANDARD METADATA COLUMNS

Every vault entity MUST include these standard metadata columns in EXACTLY this order.
BATCH_ID is mandatory on every table — it identifies which pipeline execution loaded the record.
HASHDIFF is ONLY on satellites (SAT, MSAT, ESAT) — never on hubs or links.

### Hub Columns (in this order)
1. <NOUN>_HK     BINARY(32)      NOT NULL PK  — SHA2_BINARY(256) hash of business key(s)
2. <BK_COLUMN>   source type     NOT NULL     — business key column(s)
3. LOAD_DTS      TIMESTAMP_NTZ   NOT NULL     — timestamp when record entered the vault
4. REC_SRC       VARCHAR(50)     NOT NULL     — source system code (max 5 chars, e.g. ACCTS)
5. BATCH_ID      VARCHAR(100)    NOT NULL     — pipeline batch/run identifier

### Link Columns (in this order)
1. <LNK_NAME>_HK  BINARY(32)     NOT NULL PK  — hash of all participating hub HKs
2. <HUB1_NOUN>_HK BINARY(32)     NOT NULL FK  — reference to Hub 1
3. <HUB2_NOUN>_HK BINARY(32)     NOT NULL FK  — reference to Hub 2
4. (additional hub HKs for n-ary links)
5. LOAD_DTS       TIMESTAMP_NTZ  NOT NULL     — load timestamp
6. REC_SRC        VARCHAR(50)    NOT NULL     — source system code
7. BATCH_ID       VARCHAR(100)   NOT NULL     — pipeline batch/run identifier

### Satellite Columns (SAT) (in this order)
1. <PARENT>_HK          BINARY(32)     NOT NULL PK  — FK to parent hub or link
2. LOAD_DTS             TIMESTAMP_NTZ  NOT NULL PK  — forms composite PK with parent HK
3. <ENTITY_SHORT>_HASHDIFF BINARY(32)  NOT NULL     — hash of all descriptive columns
4. REC_SRC              VARCHAR(50)    NOT NULL     — source system code
5. BATCH_ID             VARCHAR(100)   NOT NULL     — pipeline batch/run identifier
6+ <descriptive columns>                            — business attributes

### Multi-Active Satellite Columns (MSAT) (in this order)
1. <PARENT>_HK          BINARY(32)     NOT NULL PK  — FK to parent hub
2. LOAD_DTS             TIMESTAMP_NTZ  NOT NULL PK  — load timestamp
3. <DISCRIMINATOR>      source type    NOT NULL PK  — multi-active key (e.g. PHNE_TYP_CD)
4. <ENTITY_SHORT>_HASHDIFF BINARY(32)  NOT NULL     — hash of descriptive columns
5. REC_SRC              VARCHAR(50)    NOT NULL     — source system code
6. BATCH_ID             VARCHAR(100)   NOT NULL     — pipeline batch/run identifier
7+ <descriptive columns>                            — business attributes

### Effectivity Satellite Columns (ESAT) (in this order)
1. <LINK>_HK            BINARY(32)     NOT NULL PK  — FK to parent link
2. LOAD_DTS             TIMESTAMP_NTZ  NOT NULL PK  — load timestamp
3. <ENTITY_SHORT>_HASHDIFF BINARY(32)  NOT NULL     — hash of effectivity attributes
4. REC_SRC              VARCHAR(50)    NOT NULL     — source system code
5. BATCH_ID             VARCHAR(100)   NOT NULL     — pipeline batch/run identifier
6. EFF_FROM_DTS         TIMESTAMP_NTZ  NOT NULL     — relationship effective from
7. EFF_TO_DTS           TIMESTAMP_NTZ  NULL         — relationship effective to (null = current)
8+ <additional effectivity columns>

### BATCH_ID vs REC_SRC
- REC_SRC = WHAT system the data came from (static per pipeline, e.g. 'ACCTS')
- BATCH_ID = WHICH execution loaded it (dynamic per run, e.g. '20260325_ACCT_001')\
"""

SATELLITE_RULES = """\
## SATELLITE DESIGN RULES

### Source-Specific Satellites (CRITICAL RULE)
- The Raw Vault preserves data EXACTLY as delivered by each source system
- Create ONE satellite per source system per hub or link
- If two source systems deliver customer data, create TWO satellites:
    SAT_CUSTOMER_DTL__ACCTS  (from account system)
    SAT_CUSTOMER_DTL__CRM    (from CRM system)
- NEVER merge attributes from different source systems into one satellite
- Merging and reconciliation belongs in the Business Vault layer

### Choosing Satellite Type
| Condition                                          | Use   |
|----------------------------------------------------|-------|
| Standard descriptive attributes                    | SAT   |
| Multiple rows per snapshot (e.g. phone list)       | MSAT  |
| Tracks when a link relationship starts/ends        | ESAT  |

### SIMPLIFICATION GUARDRAILS (MANDATORY)
These rules prevent over-engineering. Apply them before proposing any satellite split.

1. TARGET ENTITY COUNTS per source table:
   - Hubs:       1-3  (usually 1 driving hub + reused registry hubs)
   - Links:      0-2
   - Satellites: 1-3 per hub or link
   - TOTAL:      aim for 5-8 entities; FLAG and justify if exceeding 10

2. SATELLITE CONSOLIDATION RULES:
   - If a hub/link has fewer than 8 descriptive columns total: create ONE satellite (DTL)
   - Only split into 2+ satellites when ALL of these are true:
       a) Total descriptive columns > 12
       b) There is a CLEAR, MEASURABLE difference in change frequency (e.g. FAST vs STATIC)
       c) The split meaningfully reduces ETL change detection overhead
   - NEVER create a satellite with fewer than 2 descriptive columns (excluding metadata)
   - When in doubt: consolidate into one satellite and note it in simplification_notes

3. HUB CREATION DISCIPLINE:
   - Only create a NEW hub if:
       a) The entity has a clear stable business key with high uniqueness, OR
       b) It already exists in the approved registry
   - Do NOT create a hub for a foreign key column unless that entity will clearly be
     modeled as its own hub (e.g. a CURRENCY_CD column can reference HUB_CURRENCY
     from the registry without creating a new hub)
   - Prefer REUSING an approved registry hub over creating a new one

4. LINK CREATION DISCIPLINE:
   - Only create a link when BOTH sides have (or will clearly have) their own hub
   - Do NOT create links to entities that are unlikely to be independently modeled
   - Prefer n-ary links over chains of binary links for multi-hub business events

5. Record simplification decisions in the simplification_notes field of the response.

### Satellite Splitting by Change Frequency
- FAST:   >20% of values change between loads (amounts, balances, status flags)
- SLOW:   1-20% change (names, addresses, type codes)
- STATIC: <1% change (IDs, birth dates, account open date)
- Only split when there is a CLEAR and LARGE frequency gap AND column count justifies it

### Confidence Levels
| Signal Available                          | Confidence |
|-------------------------------------------|------------|
| Profiling + column comments + modeler notes| HIGH      |
| Profiling + column comments               | HIGH       |
| Profiling only                            | MEDIUM     |
| Column names + comments, no profiling     | MEDIUM     |
| Column names only                         | LOW        |
| Table name only                           | INFERRED   |\
"""

LINK_RULES = """\
## LINK DESIGN RULES

### When to Create a Link
- Create a link when two or more DISTINCT business keys appear together in the same source record
- The link captures the relationship between the business entities at a point in time
- Example: ACCT_ID + CUST_ID in the same record -> LNK_ACCT_CUST

### Link Naming
- Nouns MUST be abbreviated using the abbreviation table — never use full words
- Nouns are in ALPHABETICAL ORDER on the abbreviated forms
- Examples: LNK_ACCT_CUST (not LNK_ACCOUNT_CUSTOMER), LNK_ACCT_TXN (not LNK_ACCOUNT_TRANSACTION)
- For n-ary links (3+ hubs): use the business event name if clearer, e.g. LNK_ACCT_CUST_PROD

### Link Hash Key
- Computed from ALL participating hub hash keys concatenated
- Sort hub hash keys alphabetically before concatenation
- Example: SHA2_BINARY(ACCOUNT_HK || '||' || CUSTOMER_HK, 256)

### Degenerate Keys
- A transaction number or document number that does not have its own hub
  goes into the link as a degenerate attribute column (not a hash key)

### Hub Reuse Detection (CRITICAL)
- Before creating a new hub, check the EXISTING APPROVED REGISTRY
- If HUB_CUSTOMER already exists and source has CUST_ID -> REUSE that hub
- Only create a new hub if NO matching business key exists in the registry
- State the reuse decision explicitly in the rationale field

### Reference Hubs
- Pre-seeded reference hubs in the registry: HUB_CURRENCY, HUB_COUNTRY, HUB_BRANCH, HUB_GL_ACCOUNT
- When a source column maps to one of these -> reference the existing hub, do NOT create a new one
- Link to reference hubs only when the relationship is meaningful for analytics\
"""

RESPONSE_FORMAT = """\
## RESPONSE FORMAT

Return a single valid JSON object. No markdown fences, no explanation outside the JSON.

INFERENCE PRIORITY (highest to lowest — honour this order strictly):
1. MODELER NOTES — treat as authoritative override for all decisions
2. MODELER CONFIRMED PK CANDIDATES — never override these
3. Column/table comments from source system (INFORMATION_SCHEMA)
4. Statistical profiling (uniqueness, nullability, patterns, change frequency)
5. Column name heuristics and abbreviation table
6. Table name inference (lowest confidence — use only as last resort)

{
  "confidence_overall": "HIGH | MEDIUM | LOW | INFERRED",
  "input_scenario": "FULL_PROFILING | COLUMN_NAMES_ONLY | DATA_INFERENCE",
  "simplification_notes": [
    "Explanation of any consolidation decisions, hub reuse, or naming abbreviations made"
  ],
  "warnings": [
    "Any low-confidence decisions, missing data, or issues the modeler should review"
  ],
  "hubs": [
    {
      "entity_id": "HUB_CUSTOMER",
      "is_new": true,
      "domain": "PARTY",
      "logical_name": "Customer Hub",
      "hash_key_name": "CUSTOMER_HK",
      "business_keys": ["CUST_ID"],
      "confidence": "HIGH | MEDIUM | LOW | INFERRED",
      "rationale": "CUST_ID has 99.8% uniqueness and 0% nulls. Strong PK candidate.",
      "columns": [
        {
          "column_name": "CUSTOMER_HK",
          "logical_name": "Customer Hash Key",
          "data_type": "BINARY(32)",
          "column_role": "HK",
          "is_nullable": false,
          "column_definition": "SHA2_BINARY(256) hash of CUST_ID."
        },
        {
          "column_name": "CUST_ID",
          "logical_name": "Customer Identifier",
          "data_type": "VARCHAR(50)",
          "column_role": "BK",
          "is_nullable": false,
          "column_definition": "Natural business key for Customer entity.",
          "source_column": "CUSTOMER_ID"
        },
        {
          "column_name": "LOAD_DTS",
          "logical_name": "Load Date Timestamp",
          "data_type": "TIMESTAMP_NTZ",
          "column_role": "META",
          "is_nullable": false,
          "column_definition": "Timestamp when the record was loaded into the vault."
        },
        {
          "column_name": "REC_SRC",
          "logical_name": "Record Source",
          "data_type": "VARCHAR(50)",
          "column_role": "META",
          "is_nullable": false,
          "column_definition": "Source system code (max 5 chars)."
        },
        {
          "column_name": "BATCH_ID",
          "logical_name": "Batch Identifier",
          "data_type": "VARCHAR(100)",
          "column_role": "META",
          "is_nullable": false,
          "column_definition": "Pipeline batch/run identifier for this load."
        }
      ]
    }
  ],
  "links": [
    {
      "entity_id": "LNK_ACCT_CUST",
      "is_new": true,
      "domain": "ACCOUNT",
      "logical_name": "Account to Customer Relationship",
      "hash_key_name": "ACCT_CUST_HK",
      "participating_hubs": ["HUB_ACCOUNT", "HUB_CUSTOMER"],
      "confidence": "HIGH",
      "rationale": "Source contains both ACCT_ID and CUST_ID establishing an account-customer relationship.",
      "columns": [
        {
          "column_name": "ACCT_CUST_HK",
          "logical_name": "Account Customer Hash Key",
          "data_type": "BINARY(32)",
          "column_role": "HK",
          "is_nullable": false,
          "column_definition": "SHA2_BINARY(256) of ACCOUNT_HK || CUSTOMER_HK."
        },
        {
          "column_name": "ACCOUNT_HK",
          "logical_name": "Account Hash Key",
          "data_type": "BINARY(32)",
          "column_role": "FK_HK",
          "is_nullable": false,
          "column_definition": "FK reference to HUB_ACCOUNT."
        },
        {
          "column_name": "CUSTOMER_HK",
          "logical_name": "Customer Hash Key",
          "data_type": "BINARY(32)",
          "column_role": "FK_HK",
          "is_nullable": false,
          "column_definition": "FK reference to HUB_CUSTOMER."
        },
        {
          "column_name": "LOAD_DTS",
          "logical_name": "Load Date Timestamp",
          "data_type": "TIMESTAMP_NTZ",
          "column_role": "META",
          "is_nullable": false,
          "column_definition": "Load timestamp."
        },
        {
          "column_name": "REC_SRC",
          "logical_name": "Record Source",
          "data_type": "VARCHAR(50)",
          "column_role": "META",
          "is_nullable": false,
          "column_definition": "Source system code."
        },
        {
          "column_name": "BATCH_ID",
          "logical_name": "Batch Identifier",
          "data_type": "VARCHAR(100)",
          "column_role": "META",
          "is_nullable": false,
          "column_definition": "Pipeline batch/run identifier."
        }
      ]
    }
  ],
  "satellites": [
    {
      "entity_id": "SAT_CUSTOMER_DTL__ACCTS",
      "is_new": true,
      "parent_entity_id": "HUB_CUSTOMER",
      "satellite_type": "SAT | MSAT | ESAT",
      "source_system": "ACCTS",
      "domain": "PARTY",
      "logical_name": "Customer Details from Account System",
      "hashdiff_name": "SAT_CUST_DTL_HASHDIFF",
      "change_frequency": "SLOW | FAST | STATIC | UNKNOWN",
      "confidence": "MEDIUM",
      "rationale": "Consolidated into one satellite — fewer than 8 descriptive columns, no meaningful change frequency split.",
      "columns": [
        {
          "column_name": "CUSTOMER_HK",
          "logical_name": "Customer Hash Key",
          "data_type": "BINARY(32)",
          "column_role": "FK_HK",
          "is_nullable": false,
          "column_definition": "FK to HUB_CUSTOMER. Part of composite PK.",
          "source_column": ""
        },
        {
          "column_name": "LOAD_DTS",
          "logical_name": "Load Date Timestamp",
          "data_type": "TIMESTAMP_NTZ",
          "column_role": "META",
          "is_nullable": false,
          "column_definition": "Load timestamp. Part of composite PK.",
          "source_column": ""
        },
        {
          "column_name": "SAT_CUST_DTL_HASHDIFF",
          "logical_name": "Customer Detail Hashdiff",
          "data_type": "BINARY(32)",
          "column_role": "HASHDIFF",
          "is_nullable": false,
          "column_definition": "SHA2_BINARY(256) hash of all descriptive columns.",
          "source_column": ""
        },
        {
          "column_name": "REC_SRC",
          "logical_name": "Record Source",
          "data_type": "VARCHAR(50)",
          "column_role": "META",
          "is_nullable": false,
          "column_definition": "Source system code.",
          "source_column": ""
        },
        {
          "column_name": "BATCH_ID",
          "logical_name": "Batch Identifier",
          "data_type": "VARCHAR(100)",
          "column_role": "META",
          "is_nullable": false,
          "column_definition": "Pipeline batch/run identifier.",
          "source_column": ""
        },
        {
          "column_name": "FRST_NM",
          "logical_name": "First Name",
          "data_type": "VARCHAR(100)",
          "column_role": "ATTR",
          "is_nullable": true,
          "column_definition": "Customer first name.",
          "source_column": "FRST_NM"
        },
        {
          "column_name": "LAST_NM",
          "logical_name": "Last Name",
          "data_type": "VARCHAR(100)",
          "column_role": "ATTR",
          "is_nullable": true,
          "column_definition": "Customer last name.",
          "source_column": "LAST_NM"
        },
        {
          "column_name": "EMAIL_ADDR",
          "logical_name": "Email Address",
          "data_type": "VARCHAR(200)",
          "column_role": "ATTR",
          "is_nullable": true,
          "column_definition": "Customer email address.",
          "source_column": "EMAIL_ADDR"
        }
      ]
    }
  ],
  "hash_definitions": [
    {
      "entity_id": "HUB_CUSTOMER",
      "hash_key_name": "CUSTOMER_HK",
      "hash_type": "BUSINESS_KEY",
      "source_columns": ["CUST_ID"],
      "null_replacement": "-1",
      "delimiter": "||",
      "algorithm": "SHA2_256",
      "preprocessing": "UPPER(TRIM(value))"
    },
    {
      "entity_id": "SAT_CUSTOMER_DTL__ACCTS",
      "hash_key_name": "SAT_CUST_DTL_HASHDIFF",
      "hash_type": "HASHDIFF",
      "source_columns": ["FRST_NM", "LAST_NM", "EMAIL_ADDR"],
      "null_replacement": "-1",
      "delimiter": "||",
      "algorithm": "SHA2_256",
      "preprocessing": "UPPER(TRIM(value)) for VARCHAR; CAST to VARCHAR for others"
    }
  ]
}

Rules:
- Return ONLY the JSON object — no text before or after
- CRITICAL: The columns array must NEVER be empty for ANY entity — not for hubs, not for links, not for satellites
- Include ALL columns: structural metadata columns (HK, FK_HK, HASHDIFF, LOAD_DTS, REC_SRC, BATCH_ID) AND every source attribute column (ATTR/BK) mapped to this entity
- For registry reused hubs (is_new=false): still include the full columns list — the modeler needs to see all columns for review
- Every entity MUST have rationale and confidence
- simplification_notes MUST explain any consolidation, hub reuse, or name abbreviation decisions
- warnings array flags low-confidence decisions, composite key uncertainty, missing data
- Hash definitions MUST be included for every HK and HASHDIFF in the proposal
- BATCH_ID must appear in every entity's column list
- MSAT_ and ESAT_ prefixes are mandatory — never use SAT_ for those types\
"""

# ── Source system abbreviations ──────────────────────────────────────────────

SOURCE_ABBRS = [
    ("ACCTS",  "Account System",           "SOURCE_SYSTEM"),
    ("CBNK",   "Core Banking System",      "SOURCE_SYSTEM"),
    ("CRM",    "CRM System",               "SOURCE_SYSTEM"),
    ("MDM",    "Master Data Management",   "SOURCE_SYSTEM"),
    ("ERP",    "ERP System",               "SOURCE_SYSTEM"),
    ("CARD",   "Card Management System",   "SOURCE_SYSTEM"),
    ("LOANS",  "Loan Origination System",  "SOURCE_SYSTEM"),
    ("RISK",   "Risk Management System",   "SOURCE_SYSTEM"),
    ("TRADE",  "Trade Finance System",     "SOURCE_SYSTEM"),
    ("PYMTS",  "Payments System",          "SOURCE_SYSTEM"),
    ("GL",     "General Ledger System",    "SOURCE_SYSTEM"),
    ("HR",     "HR System",               "SOURCE_SYSTEM"),
    ("MKTG",   "Marketing System",         "SOURCE_SYSTEM"),
    ("COMPL",  "Compliance System",        "SOURCE_SYSTEM"),
    ("DWH",    "Data Warehouse",           "SOURCE_SYSTEM"),
    ("EXTL",   "External Data Feed",       "SOURCE_SYSTEM"),
    ("YFINC",  "Yahoo Finance",            "SOURCE_SYSTEM"),
]

# ── Main ──────────────────────────────────────────────────────────────────────

conn = snowflake.connector.connect(
    account=ACCOUNT, user=USER, password=PASSWORD,
    warehouse=WAREHOUSE, database=DATABASE, schema=SCHEMA, role=ROLE
)
cs = conn.cursor()

try:
    updates = [
        ("NAMING_CONVENTIONS", NAMING_CONVENTIONS),
        ("METADATA_COLUMNS",   METADATA_COLUMNS),
        ("SATELLITE_RULES",    SATELLITE_RULES),
        ("LINK_RULES",         LINK_RULES),
        ("RESPONSE_FORMAT",    RESPONSE_FORMAT),
    ]

    print("1. Updating system prompt sections...")
    for section_name, content in updates:
        cs.execute(
            "UPDATE META.DV_AI_SYSTEM_PROMPT SET SECTION_CONTENT = %s "
            "WHERE SECTION_NAME = %s AND IS_ACTIVE = TRUE AND VERSION = '1.0'",
            (content, section_name)
        )
        rows = cs.rowcount
        print(f"   {section_name}: {rows} row(s) updated")

    print("\n2. Adding source system abbreviations to DV_ABBREVIATION...")
    for abbr, logical, domain in SOURCE_ABBRS:
        cs.execute(
            "MERGE INTO META.DV_ABBREVIATION t "
            "USING (SELECT %s AS PA, %s AS LN, %s AS DOM) s "
            "ON t.PHYSICAL_ABBR = s.PA AND t.DOMAIN = s.DOM "
            "WHEN NOT MATCHED THEN INSERT (PHYSICAL_ABBR, LOGICAL_NAME, DOMAIN, IS_ACTIVE) "
            "VALUES (s.PA, s.LN, s.DOM, TRUE)",
            (abbr, logical, domain)
        )
        print(f"   {abbr} = {logical}")

    print("\n3. Verifying section lengths...")
    cs.execute(
        "SELECT SECTION_NAME, SECTION_ORDER, LENGTH(SECTION_CONTENT) AS LEN "
        "FROM META.DV_AI_SYSTEM_PROMPT WHERE IS_ACTIVE=TRUE ORDER BY SECTION_ORDER"
    )
    for row in cs.fetchall():
        print(f"   Section {row[1]:3d}  {row[0]:<25s}  {row[2]} chars")

    print("\nAll updates complete.")

except Exception as e:
    print(f"\nERROR: {e}")
    raise
finally:
    cs.close()
    conn.close()
