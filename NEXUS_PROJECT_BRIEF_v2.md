# NEXUS DV2.0 AUTOMATION PLATFORM — Project Brief v2
## For use as Claude Code context

---

## 1. WHAT WE'RE BUILDING

An AI-assisted Data Vault 2.0 modeling and code generation platform for enterprise banking, running entirely inside Snowflake. The platform enables multiple data modelers to work in parallel, profiling source data, reviewing and editing AI-generated vault designs through a full modeling workbench, and producing DDL + dbt models — all through a Streamlit in Snowflake UI.

---

## 2. ARCHITECTURE OVERVIEW

```
┌─────────────────────────────────────────────────────────┐
│              ALL INSIDE SNOWFLAKE                        │
│                                                          │
│  Streamlit in Snowflake (Modeler Workbench)             │
│       │                                                  │
│       ▼                                                  │
│  Phase 1: Registry & Foundation Tables                   │
│       │   - META schema with all registry tables         │
│       │   - Abbreviation table, system prompt, seeds     │
│       ▼                                                  │
│  Phase 2: Data Profiling Engine                          │
│       │   - Stored procedures for profiling              │
│       │   - Table or file input                          │
│       │   - HLL for large tables                         │
│       │   - PK detection, type inference                 │
│       │   - Results → META.DV_PROFILING_RESULTS          │
│       │   - Modeler reviews and approves profiling       │
│       ▼                                                  │
│  Phase 3: AI-Assisted Raw Vault Design Workbench         │
│       │   - AI suggests model from ANY available input:  │
│       │     * Full profiling results (best)              │
│       │     * Table name + column names + definitions    │
│       │     * Column names only                          │
│       │     * Raw data inference (minimal input)         │
│       │   - Uses Cortex AI_COMPLETE (Claude Opus 4.6)    │
│       │   - Reads registry for existing vault context    │
│       │   - Shows editable model in workbench UI:        │
│       │     * Left panel: source table explorer          │
│       │     * Tab 1: metadata/profiling for source       │
│       │     * Tab 2: suggested DV model (editable)       │
│       │   - Modeler can edit table names, column names,  │
│       │     data types, definitions, add/delete columns  │
│       │   - Save, re-open, re-edit at any time           │
│       │   - "Generate Diagram" → new tab with ER diagram │
│       │     showing only PK and FK relationships         │
│       │   - Approved model → registry updated            │
│       ▼                                                  │
│  Phase 4: dbt Model Generation (TBD)                     │
│       │   - Workflow to be decided when we get there     │
│       │   - Reads approved registry                      │
│       │   - Generates staging + vault dbt models         │
│       │   - dbt in Snowflake execution                   │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

---

## 3. KEY DESIGN DECISIONS

### 3.1 Why Snowflake-Native (Not Desktop)
- Multiple modelers need shared access
- Data already in Snowflake — no egress needed
- Cortex AI_COMPLETE available natively (Claude Opus 4.6)
- Streamlit in Snowflake provides web UI with RBAC
- dbt available within Snowflake
- No separate server or credential management

### 3.2 Why Registry-Based Context (Not File-Based)
- Snowflake foreign keys are non-enforced — can't derive relationships from DDL
- Hash key computation logic not captured in DDL
- Registry is queryable and structured — AI reads it as prompt context
- Supports conflict detection across parallel modelers
- Tracks approval status, lineage, and Erwin sync state

### 3.3 Why System Prompt as Skill (Not Hardcoded)
- DV2.0 standards stored in META.DV_AI_SYSTEM_PROMPT table
- Version-controlled, auditable, editable by architecture team
- Sectioned (naming, hashing, satellites, links, response format)
- Domain-specific overrides possible
- Same standards applied regardless of which modeler runs the tool

### 3.4 Determinism Strategy
- Layer 1 (profiling): Pure SQL/Python — 100% deterministic
- Layer 2 (rules engine): Config-driven Python — 100% deterministic
- Layer 3 (AI design): Cortex with temperature=0 — near-deterministic
  - Same system prompt + same registry state + same profiling = same proposal
  - Human approval gate before anything is committed
- Layer 4 (code generation): Template-driven — 100% deterministic

### 3.5 Source-Specific Satellites (Critical Rule)
- Raw Vault preserves data exactly as delivered by each source
- One satellite per source system per hub/link — ALWAYS
- Account file with customer name/address → SAT_CUSTOMER_FROM_ACCOUNTS__ACCT_SYS
- Customer master with same fields → SAT_CUSTOMER_DETAILS__CUST_MASTER
- NEVER merge source data in Raw Vault — that's Business Vault's job

### 3.6 AI Works With Whatever It Gets
The AI design engine must handle varying levels of input:
1. **Full profiling results** — best case. PK detected, change frequency known,
   data types inferred, uniqueness ratios available
2. **Table name + column names + definitions** — no profiling. AI infers from
   column semantics and definitions. e.g., "CUST_ID" + definition "Unique
   customer identifier" → hub candidate
3. **Column names only** — no definitions, no profiling. AI infers purely from
   naming patterns using abbreviation table
4. **Raw data only** — no metadata at all. AI profiles on the fly or asks for
   a profiling run first
The quality of the suggestion degrades gracefully. AI should flag confidence
level in its response.

### 3.7 Erwin Integration
- Cannot generate native .erwin files (proprietary binary format)
- Two-track approach:
  1. Generate DDL with rich comments → Erwin reverse-engineers it
  2. Generate metadata CSV → bulk import into Erwin
- Abbreviation table (META.DV_ABBREVIATION) kept in sync with Erwin .ABR file
- Physical-to-logical name resolution via abbreviation lookup
- Erwin remains the governed master for the data model

---

## 4. DATABASE & SCHEMA STRUCTURE

```
NEXUS (database)
├── META (schema) — platform metadata and registry
│   ├── DV_ABBREVIATION — logical/physical name mapping
│   ├── DV_ENTITY — all vault entities (hubs, links, sats)
│   ├── DV_ENTITY_COLUMN — columns in each entity
│   ├── DV_RELATIONSHIP — FK relationships between entities
│   ├── DV_HASH_DEFINITION — hash key computation specs
│   ├── DV_SOURCE_MAPPING — source-to-vault column lineage
│   ├── DV_PROFILING_RESULTS — profiling output per run
│   ├── DV_PROFILING_RUN — profiling run metadata
│   ├── DV_PK_CANDIDATES — ranked PK candidates per source
│   ├── DV_DESIGN_PROPOSAL — AI-generated design proposals
│   ├── DV_DESIGN_WORKSPACE — saved modeler edits to proposals
│   ├── DV_AI_SYSTEM_PROMPT — the "skill" / standards
│   └── DV_AUDIT_LOG — all actions logged for governance
│
├── STAGING (schema) — source data lands here
│   └── (source tables or loaded files)
│
├── RAW_VAULT (schema) — generated hubs, links, satellites
│
└── BUSINESS_VAULT (schema) — future: PIT, bridge, computed sats
```

---

## 5. BUILD PHASES

### Phase 1: Registry & Foundation Tables
**What:** Create all META tables, seed abbreviations, reference hubs, AI system prompt.
**Status:** DDL script created (phase_1_registry_ddl.sql). Deploy to Snowflake.
**Tables:**
- DV_ABBREVIATION (with ~70 banking seed terms)
- DV_ENTITY, DV_ENTITY_COLUMN, DV_RELATIONSHIP
- DV_HASH_DEFINITION, DV_SOURCE_MAPPING
- DV_PROFILING_RUN, DV_PROFILING_RESULTS, DV_PK_CANDIDATES
- DV_DESIGN_PROPOSAL, DV_DESIGN_WORKSPACE
- DV_AI_SYSTEM_PROMPT, DV_AUDIT_LOG
- Seed: HUB_CURRENCY, HUB_COUNTRY, HUB_BRANCH, HUB_GL_ACCOUNT

### Phase 2: Data Profiling Engine + UI
**What:** Stored procedures for profiling + Streamlit screens for review.
**Components:**
- SP_PROFILE_TABLE(schema, table, run_id) — main profiler
- SP_DETECT_PK_CANDIDATES(run_id) — PK scoring
- SP_DETECT_CHANGE_FREQUENCY(schema, table, run_id) — if snapshots available
- Streamlit: table selector, profile trigger, results review, PK confirmation

### Phase 3: AI-Assisted Raw Vault Design Workbench
**What:** The core modeling experience. AI suggests, modeler refines, saves, generates diagram.
**This is the most detailed phase — see Section 6 below for full specification.**

### Phase 4: dbt Model Generation (TBD)
**What:** Generate dbt models from approved registry. Workflow to be decided later.
**Anticipated components:**
- dbt macro library (generate_hub, generate_link, generate_satellite, etc.)
- Staging model generator
- dbt in Snowflake execution
- Testing and validation

---

## 6. PHASE 3 DETAILED SPECIFICATION: AI Design Workbench

### 6.1 Overview

The design workbench is a Streamlit in Snowflake application with three main areas:

```
┌──────────────┬─────────────────────────────────────────────┐
│              │                                             │
│   SOURCE     │          MAIN WORK AREA                     │
│   EXPLORER   │                                             │
│              │  ┌─────────────┬──────────────┬──────────┐  │
│  ○ ACCT_MSTR │  │  Metadata   │  DV Model    │ Diagram  │  │
│  ○ CUST_MSTR │  │  /Profiling │  (editable)  │          │  │
│  ● TXN_HIST  │  │             │              │          │  │
│              │  │             │              │          │  │
│              │  │             │              │          │  │
│              │  └─────────────┴──────────────┴──────────┘  │
│              │                                             │
│  [+ Add      │  [Save] [Re-generate] [Approve] [Export]   │
│   Source]    │                                             │
└──────────────┴─────────────────────────────────────────────┘
```

### 6.2 Left Panel: Source Explorer

- Lists all source tables that have been profiled or submitted for modeling
- Each entry shows:
  - Source table name
  - Source system
  - Status icon: ● Profiled / ○ Metadata only / ◑ Model generated / ✓ Approved
  - Last profiled date (if profiled)
  - Last modified date
- Clicking a source table loads its details in the main work area
- [+ Add Source] button: allows modeler to point to a new staging table
  or provide metadata manually (table name, column names, definitions)

### 6.3 Tab 1: Metadata / Profiling

When a source table is selected, this tab shows:

**If profiling was done:**
- Table-level stats: row count, column count, profiling date
- Per-column table:
  | Column Name | Source Type | Inferred Type | Unique % | Null % | PK? | Change Freq | Top Values |
- PK candidates with scores and modeler selection
- All read-only (edits happen in profiling review screens from Phase 2)

**If only metadata was provided (no profiling):**
- Column names, definitions, declared data types, declared PKs
- Flag indicating "Not profiled — AI suggestions based on metadata only"

**If neither metadata nor profiling exists:**
- Message: "No metadata available. AI will infer from data and column names."
- Option to trigger profiling or manually enter metadata

### 6.4 Tab 2: DV Model (Editable Workbench)

This is the core modeling interface. After AI generates a suggestion (or on first load
if a saved workspace exists), this tab shows the proposed Raw Vault model.

**Layout: Grouped by entity, each entity is a collapsible card.**

```
┌─────────────────────────────────────────────────────────┐
│ ▼ HUB_CUSTOMER                              [Delete]    │
│   Status: NEW | Domain: PARTY                           │
│   Logical Name: [Customer Hub____________]              │
│   ┌──────────────┬──────────────┬───────────┬─────────┐ │
│   │ Column Name  │ Data Type    │ Role      │ Defn    │ │
│   ├──────────────┼──────────────┼───────────┼─────────┤ │
│   │ [CUSTOMER_HK]│ BINARY(32)   │ HK        │ [Hash..]│ │
│   │ [CUST_ID   ] │ [VARCHAR(50)]│ BK        │ [Cust..]│ │
│   │ [LOAD_DTS  ] │ TIMESTAMP_NTZ│ META      │ [Load..]│ │
│   │ [REC_SRC   ] │ VARCHAR(100) │ META      │ [Sourc.]│ │
│   └──────────────┴──────────────┴───────────┴─────────┘ │
│   [+ Add Column]                                        │
│   Source: ACCT_MSTR.CUSTOMER_ID → CUST_ID               │
│   Rationale: "CUSTOMER_ID has 99.8% uniqueness, 0% ..." │
├─────────────────────────────────────────────────────────┤
│ ▼ SAT_CUSTOMER_DETAILS__ACCT_SYS            [Delete]    │
│   Status: NEW | Parent: HUB_CUSTOMER                    │
│   Logical Name: [Customer Details from Account System_] │
│   Type: STANDARD | Change Freq: SLOW                    │
│   ┌──────────────┬──────────────┬───────────┬─────────┐ │
│   │ Column Name  │ Data Type    │ Role      │ Defn    │ │
│   ├──────────────┼──────────────┼───────────┼─────────┤ │
│   │ [CUSTOMER_HK]│ BINARY(32)   │ FK_HK     │ [Hash..]│ │
│   │ [FRST_NM   ] │ [VARCHAR(100)]│ ATTR     │ [First.]│ │
│   │ [LST_NM    ] │ [VARCHAR(100)]│ ATTR     │ [Last..]│ │
│   │ [EMAIL_ADDR] │ [VARCHAR(200)]│ ATTR     │ [Email.]│ │
│   └──────────────┴──────────────┴───────────┴─────────┘ │
│   [+ Add Column]                                        │
│   Rationale: "Slow-changing descriptive attributes..."   │
├─────────────────────────────────────────────────────────┤
│ ► LNK_ACCOUNT_CUSTOMER (collapsed — click to expand)    │
├─────────────────────────────────────────────────────────┤
│ [+ Add Entity]                                          │
└─────────────────────────────────────────────────────────┘
```

**Editable fields (shown in brackets [ ] above):**
- Entity/table name — modeler can rename
- Logical name — free text
- Column name — modeler can rename
- Data type — dropdown or free text
- Column definition — free text
- Domain assignment — dropdown

**Actions per entity:**
- [Delete] — remove this entity from the proposal
- [+ Add Column] — add a new column to this entity

**Actions per column:**
- Each column row has a delete (x) button
- Click column name to edit inline
- Click data type to change via dropdown
- Click definition to edit inline

**Global actions (bottom of tab):**
- [+ Add Entity] — manually add a hub, link, or satellite
- [Save] — saves current state to DV_DESIGN_WORKSPACE (can re-open later)
- [Re-generate] — re-runs AI with updated context (warns: will overwrite edits)
- [Approve All] — marks all entities as approved, writes to registry
- [Export DDL] — generates DDL SQL for download
- [Export Erwin CSV] — generates Erwin-compatible metadata CSV

**Visual indicators:**
- NEW entities shown with green left border
- EXISTING (reused from registry) entities shown with blue left border
- MODIFIED (edited from AI suggestion) shown with orange left border
- Warnings/conflicts shown with red banner at top of entity card

### 6.5 Tab 3: Diagram

- This tab is initially empty with a [Generate Diagram] button
- On click, reads all entities from the current workspace/proposal
- Generates an ER diagram showing:
  - Each entity as a box with entity name and type badge (HUB/LNK/SAT/MSAT/ESAT)
  - Inside each box: only PK columns and FK columns (not all attributes)
  - Lines between entities showing FK relationships:
    - Hub → Link (HK reference)
    - Hub → Satellite (HK reference)
    - Link → Satellite (HK reference)
  - Cardinality notation on relationship lines
- Color coding:
  - Hubs: blue
  - Links: green
  - Satellites: orange
  - Multi-Active Satellites: purple
  - Effectivity Satellites: red
- Implementation options:
  - Mermaid.js ER diagram (text-driven, easy to generate)
  - Or custom SVG/D3 visualization
  - Or PyVis/NetworkX rendered as HTML
- Diagram should be zoomable and pannable
- Click on entity in diagram → navigates to that entity in Tab 2

### 6.6 AI Input Flexibility

The AI design stored procedure must handle four input scenarios:

**Scenario A: Full profiling available**
```
Input to AI: profiling results (uniqueness, nulls, types, PK candidates,
change frequency) + column names + registry context
AI confidence: HIGH
```

**Scenario B: Table name + column names + definitions (no profiling)**
```
Input to AI: column names, declared definitions, declared types, declared PKs
+ registry context
AI confidence: MEDIUM
AI uses: abbreviation table to parse column names, definitions for semantic
understanding, registry for hub reuse detection
```

**Scenario C: Column names only (no definitions, no profiling)**
```
Input to AI: just the list of column names + registry context
AI confidence: LOW
AI uses: abbreviation table to decompose names (CUST_ACCT_BAL_AMT →
Customer Account Balance Amount), naming patterns (columns ending in _ID
are likely keys), registry for matching existing hub business keys
```

**Scenario D: Raw data only (infer everything)**
```
Input to AI: sample data rows + registry context
AI must: infer column semantics from data patterns, detect keys from
uniqueness, infer types from values
AI confidence: VERY LOW — heavy warning banner for modeler
Recommendation: suggest running full profiling first
```

For each scenario, the AI response JSON should include a `confidence` field
per entity: HIGH / MEDIUM / LOW / INFERRED. The UI shows this as a badge
on each entity card so the modeler knows where to focus review attention.

### 6.7 Save and Re-edit Workflow

**DV_DESIGN_WORKSPACE table** stores the modeler's working state:

```sql
CREATE TABLE META.DV_DESIGN_WORKSPACE (
    WORKSPACE_ID      VARCHAR(200)    NOT NULL,
    PROPOSAL_ID       VARCHAR(200),    -- original AI proposal if any
    SOURCE_TABLE      VARCHAR(100)    NOT NULL,
    SOURCE_SYSTEM     VARCHAR(50)     NOT NULL,
    WORKSPACE_JSON    VARIANT         NOT NULL,  -- full editable state
    STATUS            VARCHAR(20)     DEFAULT 'DRAFT',
    LAST_MODIFIED_BY  VARCHAR(100)    DEFAULT CURRENT_USER(),
    CREATED_DATE      TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    LAST_MODIFIED     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_DV_WORKSPACE PRIMARY KEY (WORKSPACE_ID),
    CONSTRAINT CK_WS_STATUS CHECK (STATUS IN (
        'DRAFT', 'IN_REVIEW', 'APPROVED', 'SUPERSEDED'
    ))
);
```

- Modeler opens workbench → loads from DV_DESIGN_WORKSPACE if draft exists
- Every [Save] writes current state as JSON to workspace
- [Approve All] writes to DV_ENTITY, DV_ENTITY_COLUMN, DV_RELATIONSHIP,
  DV_HASH_DEFINITION, DV_SOURCE_MAPPING and marks workspace as APPROVED
- Modeler can re-open an approved workspace and create a new version
  (original marked SUPERSEDED)

### 6.8 Multi-Modeler Considerations

- Each workspace is owned by its creator (LAST_MODIFIED_BY)
- Source explorer shows all workspaces across all modelers
- Conflict detection: if Modeler A has a draft workspace that creates HUB_CUSTOMER,
  and Modeler B tries to create HUB_CUSTOMER from a different source, the system
  warns about the conflict
- Registry entries are only visible to AI context when STATUS = 'APPROVED',
  so draft work doesn't pollute other modelers' AI suggestions
- Audit log tracks all saves and approvals

---

## 7. PROFILING LOGIC DETAIL

### Per-Column Metrics:
| Metric | How Computed | Purpose |
|--------|-------------|---------|
| ROW_COUNT | COUNT(*) | Scale awareness |
| DISTINCT_COUNT | COUNT(DISTINCT) or HLL() if >10M rows | Uniqueness |
| UNIQUENESS_RATIO | DISTINCT_COUNT / ROW_COUNT | PK candidacy |
| NULL_COUNT | COUNT_IF(col IS NULL) | Data quality |
| NULL_PERCENTAGE | NULL_COUNT / ROW_COUNT | PK disqualifier |
| MIN_LENGTH | MIN(LEN(col)) | Type inference |
| MAX_LENGTH | MAX(LEN(col)) | VARCHAR sizing |
| AVG_LENGTH | AVG(LEN(col)) | Type inference |
| MIN_VALUE | MIN(col) | Range analysis |
| MAX_VALUE | MAX(col) | Range analysis |
| TOP_5_VALUES | APPROX_TOP_K(col, 5) | Pattern detection |
| INFERRED_DATA_TYPE | Attempt CAST to NUMBER/DATE/TIMESTAMP/BOOLEAN | Type detection |
| PATTERN_DETECTED | REGEXP pattern analysis | Format detection |

### PK Candidate Scoring:
| Criterion | Score |
|-----------|-------|
| Uniqueness ratio = 1.0 | +40 |
| Uniqueness ratio > 0.95 | +25 |
| Null percentage = 0 | +20 |
| Column name contains ID/KEY/NBR/NUM/CODE | +15 |
| Data type is numeric or short varchar | +10 |
| Column is first in table ordinal position | +5 |
| Composite (2-col) with uniqueness = 1.0 | +30 |
| Composite (3-col) with uniqueness = 1.0 | +20 |

Threshold: Score >= 60 = strong candidate, 40-59 = possible, <40 = unlikely

### Change Frequency Detection:
- If historical snapshots available: compare column values across snapshots
- FAST: >20% of values change between snapshots
- SLOW: 1-20% of values change
- STATIC: <1% change
- If no snapshots: classify by data semantics
  - Amounts, balances, rates, status → assumed FAST
  - Names, types, codes → assumed SLOW
  - IDs, birth dates, SSN → assumed STATIC

---

## 8. NAMING CONVENTIONS

### Entity Naming:
- Hubs: HUB_<BUSINESS_NOUN>
- Links: LNK_<NOUN1>_<NOUN2> (alphabetical order)
- Satellites: SAT_<PARENT_ENTITY_NOUN>_<DESCRIPTOR>__<SOURCE_SYSTEM>
- Multi-Active Sats: MSAT_<PARENT>_<DESCRIPTOR>__<SOURCE>
- Effectivity Sats: ESAT_<LINK_NAME>__<SOURCE>
- PIT Tables: PIT_<HUB_NAME>
- Bridge Tables: BRG_<BUSINESS_CONCEPT>

### Column Naming:
- Hash Keys: <NOUN>_HK (BINARY(32))
- Business Keys: use abbreviated form from DV_ABBREVIATION
- Hashdiff: <SAT_NAME>_HASHDIFF (BINARY(32))
- Load timestamp: LOAD_DTS (TIMESTAMP_NTZ)
- Record source: REC_SRC (VARCHAR(100))
- Effectivity dates: EFF_FROM_DTS, EFF_TO_DTS

### Hash Key Computation:
- Algorithm: SHA2_BINARY(256)
- Null handling: COALESCE(CAST(col AS VARCHAR), '-1')
- Multi-column: concatenate with '||' delimiter
- Column order: alphabetical by column name
- Pre-processing: UPPER(TRIM(value)) before hashing

---

## 9. CORTEX AI INTEGRATION

### System Prompt Storage
- Stored in META.DV_AI_SYSTEM_PROMPT table
- Sectioned: ROLE, NAMING_CONVENTIONS, HASH_STANDARDS, SATELLITE_RULES,
  METADATA_COLUMNS, LINK_RULES, RESPONSE_FORMAT
- Assembled at runtime by LISTAGG ordered by SECTION_ORDER
- Passed as system message to AI_COMPLETE

### AI Call Pattern
```sql
SNOWFLAKE.CORTEX.AI_COMPLETE(
    'claude-opus-4-6',
    [
        {'role': 'system', 'content': <assembled system prompt>},
        {'role': 'user', 'content': <registry context + profiling/metadata + request>}
    ],
    {'temperature': 0, 'max_tokens': 8192}
);
```

### AI Response Format
The AI returns JSON with: hubs, links, satellites, hash_definitions, warnings.
Each entity includes: entity_id, is_new, domain, logical_name, columns,
hash_key_name, rationale, confidence (HIGH/MEDIUM/LOW/INFERRED).

---

## 10. dbt MACRO LIBRARY (Phase 4 — to be built later)

### Core Macros:
- generate_hash_key(columns, alias) → SHA2 hash expression
- generate_hub(hub_name, source_model, business_keys, hash_key, record_source)
- generate_link(link_name, source_model, hub_references, hash_key, record_source)
- generate_satellite(sat_name, source_model, parent_hash_key, hashdiff_columns,
  descriptive_columns, record_source)
- generate_multiactive_satellite(..., multi_active_key)
- generate_effectivity_satellite(...)
- generate_staging(source_table, derived_columns, record_source)

### dbt Model Structure:
```
models/
├── staging/<source_system>/
│   └── stg_<source>__<table>.sql     ← view, computes all hashes
├── raw_vault/
│   ├── hubs/hub_<noun>.sql            ← incremental, calls generate_hub()
│   ├── links/lnk_<n1>_<n2>.sql        ← incremental, calls generate_link()
│   └── sats/sat_<n>__<src>.sql        ← incremental, calls generate_satellite()
└── business_vault/ (future)
    ├── pit/pit_<hub>.sql
    ├── bridge/brg_<concept>.sql
    └── computed_sats/sat_<n>.sql
```

---

## 11. TECHNOLOGY STACK

| Component | Technology |
|-----------|-----------|
| Database | Snowflake |
| AI Engine | Cortex AI_COMPLETE (Claude Opus 4.6) |
| UI | Streamlit in Snowflake |
| DV2.0 Standards | META.DV_AI_SYSTEM_PROMPT (system prompt as skill) |
| Profiling | Snowflake Stored Procedures (SQL + Snowpark Python) |
| Model Registry | Snowflake tables (META schema) |
| Code Generation | Snowflake Stored Procedures (template-driven) |
| dbt | dbt in Snowflake |
| ER Diagram | Mermaid.js / D3 / PyVis in Streamlit |
| Data Modeling Tool | Erwin (via DDL reverse engineering + CSV import) |
| Version Control | Git (for dbt models and generated artifacts) |
| Development Tool | Claude Code in VS Code (build-time only) |

---

## 12. INSTRUCTIONS FOR CLAUDE CODE

When building this platform in Claude Code:

1. **Read this entire brief first** before starting any phase
2. **Phase 1** is already built (phase_1_registry_ddl.sql) — deploy to Snowflake
   - NOTE: Add DV_DESIGN_WORKSPACE table to Phase 1 DDL
3. **Phase 2** — build profiling stored procedures first, then Streamlit UI
   - Test each SP against real or sample Snowflake data
   - Streamlit screens: table selector → profile trigger → results review → PK confirm
4. **Phase 3** — build in this order:
   a. AI proposal stored procedure (SP_GENERATE_DV_PROPOSAL)
   b. Source explorer (left panel)
   c. Metadata/profiling tab
   d. DV Model editable tab (most complex — entity cards with inline editing)
   e. Save/load workspace logic
   f. Diagram generation tab
5. **Phase 4** — TBD, will be decided after Phase 3 is working

For each phase:
- Generate SQL as .sql files for stored procedures
- Generate Python as .py files for Streamlit app
- Include comments explaining each component
- Include GRANT statements for RBAC
- Include test/validation queries
- All objects in NEXUS database, META schema

### Streamlit App Structure
The Streamlit app should be structured as a multi-page app:
- Page 1: Source Table Management (select, profile, add metadata)
- Page 2: Profiling Review (Phase 2 screens)
- Page 3: Design Workbench (Phase 3 — the main modeling experience)
Each page shares session state for the selected source table.
