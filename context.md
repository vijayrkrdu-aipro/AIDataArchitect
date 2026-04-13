# NEXUS DV2.0 — Project Context Reference

> **Purpose:** Complete project context for Claude Code. Reference this file instead of loading all source files.  
> **Last updated:** 2026-04-10

---

## 1. What This Is

An AI-assisted **Data Vault 2.0 automation platform** running entirely inside Snowflake, used by enterprise banking data modelers. It profiles source tables, generates Raw Vault designs using Claude via Cortex, provides an editable modeling workbench, and generates DDL + dbt models.

**Tech stack:** Snowflake + Cortex AI_COMPLETE (claude-opus-4-6) + Streamlit in Snowflake + dbt in Snowflake  
**Target users:** Multiple data modelers working in parallel  
**Deployment:** Everything inside Snowflake. Local Python scripts deploy SQL/SP files to Snowflake.

---

## 2. Repository Structure

```
AIDataArchitect/
├── .env                          # SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD
├── NEXUS_PROJECT_BRIEF_v2.md     # Original full project spec (authoritative reference)
├── context.md                    # THIS FILE
├── phase_1/
│   ├── 01_foundation_ddl.sql     # NEXUS DB, all META tables — run first
│   ├── 02_seed_abbreviations.sql # ~70 banking abbreviations → META.DV_ABBREVIATION
│   ├── 03_seed_system_prompt.sql # DV2.0 standards → META.DV_AI_SYSTEM_PROMPT (7 sections)
│   └── 04_seed_reference_hubs.sql# Pre-seeded HUB_CURRENCY, HUB_COUNTRY, HUB_BRANCH, HUB_GL_ACCOUNT
├── phase_1_addendum_workspace.sql# Addendum DDL (DV_DESIGN_WORKSPACE additions)
├── phase_2/
│   ├── 01_sp_profile_table.sql       # META.SP_PROFILE_TABLE(schema,table,db,system,run_id,pk_cols)
│   ├── 02_sp_detect_pk_candidates.sql# META.SP_DETECT_PK_CANDIDATES(run_id)
│   ├── 03_sp_detect_change_frequency.sql # META.SP_DETECT_CHANGE_FREQUENCY(run_id)
│   ├── 04_streamlit_app.py           # THE ENTIRE UI — Streamlit in Snowflake app
│   ├── 05_migration_add_source_db.sql# Migration: added SOURCE_DATABASE column to profiling tables
│   ├── deploy_sp.py                  # Deploys phase_2 SPs to Snowflake
│   └── deploy_streamlit.py           # Uploads 04_streamlit_app.py to Snowflake stage + creates/replaces app
├── phase_3/
│   ├── 01_sp_generate_dv_proposal.sql# META.SP_GENERATE_DV_PROPOSAL(table,system,schema,db,run_id,notes)
│   ├── deploy_phase3.py              # Deploys SP + updates Streamlit app
│   └── update_system_prompt.py       # Updates META.DV_AI_SYSTEM_PROMPT from local SQL
└── build_ppt.py                  # (unrelated — PPT generation script)
```

---

## 3. Snowflake Objects

### Database & Schemas
```
NEXUS (database)
├── META          — platform registry, SPs, all metadata
├── STAGING       — source data landing zone
├── RAW_VAULT     — generated vault tables (hubs, links, sats)
└── BUSINESS_VAULT— future: PIT, bridge, computed sats
```

### META Tables (all in NEXUS.META)

| Table | Purpose |
|-------|---------|
| `DV_ABBREVIATION` | Physical→logical name mapping (sync'd with Erwin .ABR). ~70 banking terms |
| `DV_ENTITY` | Registry of all vault entities. AI only reads APPROVED rows as context |
| `DV_ENTITY_COLUMN` | Columns per entity. COLUMN_ROLE: HK/BK/FK_HK/HASHDIFF/META/ATTR/MAK |
| `DV_RELATIONSHIP` | Explicit FK relationships (Snowflake FKs are non-enforced) |
| `DV_HASH_DEFINITION` | Exact hash computation spec per hash key (algo, cols, delimiter, preprocessing) |
| `DV_SOURCE_MAPPING` | Source column → vault entity column lineage |
| `DV_PROFILING_RUN` | One row per profiling execution. Has SOURCE_DATABASE column (added in migration) |
| `DV_PROFILING_RESULTS` | Per-column stats per run (uniqueness, nulls, types, top values, pattern, change freq) |
| `DV_PK_CANDIDATES` | Scored PK candidates per run. Supports MODELER_SUGGESTED and AI_RECOMMENDED types |
| `DV_DESIGN_PROPOSAL` | Raw AI JSON response per generation call |
| `DV_DESIGN_WORKSPACE` | Modeler's editable working state (WORKSPACE_JSON VARIANT). Statuses: DRAFT/IN_REVIEW/APPROVED/SUPERSEDED |
| `DV_AI_SYSTEM_PROMPT` | DV2.0 standards as 7 ordered sections. Assembled at runtime via LISTAGG. Version='1.0' |
| `DV_AUDIT_LOG` | Immutable governance trail |
| `DV_COLUMN_DEFINITIONS` | Modeler-approved column definitions. Persists across sessions for datashares/read-only sources |
| `DV_DDL_EXPORT` | Generated DDL saved for retrieval (best-effort) |
| `DV_ERWIN_EXPORT` | Erwin export rows — queryable from Snowsight, download as CSV/Excel |

### Stored Procedures (all in NEXUS.META)

| SP | Signature | Returns | Notes |
|----|-----------|---------|-------|
| `SP_PROFILE_TABLE` | `(schema, table, db, system, run_id, pk_cols_json)` | void | Snowpark Python. HLL for >10M rows |
| `SP_DETECT_PK_CANDIDATES` | `(run_id)` | void | Scores single + composite PK candidates |
| `SP_DETECT_CHANGE_FREQUENCY` | `(run_id)` | void | Snapshot comparison if _HIST/_SNAP table exists, else name-pattern heuristics |
| `SP_GENERATE_DV_PROPOSAL` | `(table, system, schema, db, run_id, modeler_notes)` | `workspace_id VARCHAR` | Calls Cortex claude-opus-4-6, saves to DV_DESIGN_PROPOSAL + DV_DESIGN_WORKSPACE |

---

## 4. The Streamlit App (`phase_2/04_streamlit_app.py`)

Single file, ~3700 lines. Runs as **Streamlit in Snowflake** (not local).  
Uses `get_active_session()` — no credentials needed inside Snowflake.

### Navigation (sidebar radio)
1. **Identify Source** — browse any DB/schema/table, trigger profiling, manage runs
2. **Profile and Review** — column stats, AI analysis tab, PK candidates tab
3. **Design Raw Vault** — entity workbench, save/approve models, diagram
4. **Generate Erwin** — DDL + Erwin Excel export for approved models
5. **Generate DBT** — automate_dv dbt model files

### Key Page Functions

#### `page_source_tables()` — "Identify Source"
- Cascading DB→Schema→Table selectors (multi-table profiling supported)
- Source system code (max 5 chars) required before profiling
- Calls `SP_PROFILE_TABLE`, `SP_DETECT_PK_CANDIDATES`, `SP_DETECT_CHANGE_FREQUENCY`
- Shows previous runs for single-table selection with "Open in Profiling Review" button

#### `page_profiling_review()` — "Profile and Review"
**Tabs:**
- **Column Statistics:** styled dataframe with change freq + sensitivity colour coding
- **AI Analysis:** runs Cortex (user picks model: Haiku/Sonnet/Opus). Outputs:
  - Dataset Overview (section 1), Column Definitions (section 2), Business Key (section 3), Data Category (section 4), Sensitivity (section 5), Context Validation (section 6 if modeler provided context)
  - Parsed structured outputs: `RECOMMENDED_BK`, `DATA_CATEGORY`, `CORE_CONCEPTS`, `SENSITIVITY: COL: CATEGORY`
  - Side-by-side Vanilla vs Enriched column defs (editable grid)
  - "Save Definitions" → MERGE into `META.DV_COLUMN_DEFINITIONS`, best-effort ALTER COLUMN COMMENT on source
  - AI vault notes cached in `st.session_state["ai_vault_notes"][src_key]` for Design Workbench
- **PK Candidates:** AI-recommended BK + statistically detected candidates, confirm/override UI

#### `page_design_workbench()` — "Design Raw Vault"
- Left panel: "Model Generated" tab (existing workspaces) + "Ready for Model" tab (profiled but not yet modeled)
- Right panel: loads workspace JSON, renders entity cards via `_render_entity_card()`
  - Entity cards: inline editing of name, columns (add/delete/edit), roles, source columns
  - **Save Workspace** → `_save_workspace()` → UPDATE DV_DESIGN_WORKSPACE
  - **Re-generate** → supersedes current workspace, calls SP_GENERATE_DV_PROPOSAL
  - **Approve All** → `_approve_workspace()` → writes to DV_ENTITY, DV_ENTITY_COLUMN, DV_HASH_DEFINITION, marks workspace APPROVED
- `_merge_vault_notes(src_key, modeler_notes)` — builds the `modeler_notes` string passed to the SP; pulls AI notes from session state AND saved defs from DV_COLUMN_DEFINITIONS
- Diagram tab: Graphviz DOT generation via `_generate_dot(ws)`
- Widget state keys prefixed `__wb_` — cleared via `_clear_wb_widget_state()` on workspace switch

#### `page_generate_model()` — "Generate Erwin"
- Loads approved entities filtered by DB→Schema→Source Table
- Topology sort: HUBs → LINKs → SATs
- Generates CREATE TABLE DDL with FK constraints
- `.sql`: displayed in st.code block (copy button), saved to `META.DV_DDL_EXPORT`
- Erwin Excel: written to `META.DV_ERWIN_EXPORT` with EXPORT_ID, queryable from Snowsight
- Git push: GitHub API via urllib (PUT to contents endpoint)
- Column definitions overlaid: `DV_COLUMN_DEFINITIONS` beats `DV_ENTITY_COLUMN.COLUMN_DEFINITION` for ATTR/BK columns

#### `page_generate_dbt()` — "Generate DBT"
**3 sections:**
1. **One-time setup files:** `dbt_project.yml`, `packages.yml`, `profiles.yml` — shown in code tabs
2. **Combined YAML** (all approved tables): `sources.yml` + `schema.yml` — one button generates both
3. **Per-table SQL files:** staging view + vault entity SQL using automate_dv macros

**Hash in staging:** Uses `MD5_BINARY()` directly (matches `BINARY(16)` — note: automate_dv uses MD5 not SHA256 at staging layer).  
**automate_dv macros used:** `hub()`, `link()`, `sat()`, `ma_sat()`  
**dbt project name:** `nexusdbt`  
**Schemas:** staging → `STG`, vault → `RAW_VAULT`

---

## 5. AI Integration Details

### Cortex Call Pattern
```sql
SELECT SNOWFLAKE.CORTEX.AI_COMPLETE(
    ?,                                    -- model name (bind param)
    PARSE_JSON(?),                        -- messages JSON (bind param — avoids escaping issues)
    OBJECT_CONSTRUCT('max_tokens', 20000, 'temperature', 0)
)::VARCHAR AS ANALYSIS
```
**Critical:** options object must be a literal `OBJECT_CONSTRUCT`, NOT `PARSE_JSON(?)`.

### Models available (AI Analysis tab)
- `claude-haiku-4-5` — fastest
- `claude-sonnet-4-6` — default (balanced)
- `claude-opus-4-6` — best quality (also used by SP_GENERATE_DV_PROPOSAL)

### System Prompt (META.DV_AI_SYSTEM_PROMPT, version='1.0')
7 sections assembled via `LISTAGG(SECTION_CONTENT) WITHIN GROUP (ORDER BY SECTION_ORDER)`:

| # | Section | Key content |
|---|---------|-------------|
| 10 | ROLE | Senior DV2.0 architect, banking/finance domain |
| 20 | NAMING_CONVENTIONS | Entity patterns (HUB/LNK/SAT), column patterns, abbreviation rules |
| 30 | HASH_STANDARDS | SHA2_BINARY(256), BINARY(32), null replacement '-1', alphabetical col order |
| 40 | METADATA_COLUMNS | Required columns per entity type (Hub/Link/Sat/MSAT/ESAT) in order |
| 50 | SATELLITE_RULES | Source-specific sats (critical), SAT/MSAT/ESAT selection, change freq splitting |
| 60 | LINK_RULES | When to create, degenerate keys, hub reuse detection, reference hubs |
| 70 | RESPONSE_FORMAT | Strict JSON schema with hubs/links/satellites/hash_definitions arrays |

### AI Response JSON Schema (from SP_GENERATE_DV_PROPOSAL)
```json
{
  "confidence_overall": "HIGH|MEDIUM|LOW|INFERRED",
  "input_scenario": "FULL_PROFILING|METADATA_ONLY|COLUMN_NAMES_ONLY|DATA_INFERENCE",
  "warnings": [...],
  "hubs": [{entity_id, is_new, domain, logical_name, hash_key_name, business_keys, confidence, rationale, columns: [...]}],
  "links": [{entity_id, is_new, participating_hubs, confidence, rationale, columns: [...]}],
  "satellites": [{entity_id, is_new, parent_entity_id, satellite_type, source_system, change_frequency, confidence, rationale, columns: [...]}],
  "hash_definitions": [{entity_id, hash_key_name, hash_type, source_columns, null_replacement, delimiter, algorithm, preprocessing}]
}
```
Column object fields: `column_name, logical_name, data_type, column_role, is_nullable, column_definition, source_column`

---

## 6. Naming Conventions

### Entity naming
- Hub: `HUB_<NOUN>` e.g. `HUB_CUSTOMER`
- Link: `LNK_<NOUN1>_<NOUN2>` (alphabetical) e.g. `LNK_ACCOUNT_CUSTOMER`
- Satellite: `SAT_<SRCCODE>_<NOUN>_<DESCRIPTOR>` e.g. `SAT_ACCTS_CUSTOMER_DETAILS`
- Multi-Active Sat: `MSAT_<SRCCODE>_<NOUN>_<DESCRIPTOR>`
- Effectivity Sat: `ESAT_<SRCCODE>_<LINK_NAME>`
- Source system code: max 5 chars, comes FIRST after `SAT_`/`MSAT_`/`ESAT_`, single underscores throughout

### Column naming
- Hash Key: `<NOUN>_HK` (BINARY(32)) — primary key of hub
- Link HK: `<NOUN1>_<NOUN2>_HK` (BINARY(32))
- Hashdiff: `<ABBREVIATED_SAT>_HASHDIFF` (BINARY(32))
- Load timestamp: `LOAD_DTS` (TIMESTAMP_NTZ)
- Record source: `REC_SRC` (VARCHAR(100))
- Batch ID: `BATCH_ID` (VARCHAR(100))
- BK/ATTR: **exact source column name** — do NOT rename or abbreviate

### Hash computation
- Algorithm: SHA2_BINARY(256) → BINARY(32)
- Null replacement: `COALESCE(CAST(col AS VARCHAR), '-1')`
- Concatenation delimiter: `||`
- Column order: **alphabetical**
- Preprocessing: `UPPER(TRIM(value))` on all char columns

---

## 7. Profiling Logic

### PK Scoring (cumulative, score ≥60 = strong)
| Criterion | Points |
|-----------|--------|
| Uniqueness = 1.0 | +40 |
| Uniqueness > 0.95 | +25 |
| Null % = 0 | +20 |
| Name contains ID/KEY/NBR/NUM/CODE | +15 |
| Numeric or short varchar type | +10 |
| First ordinal position | +5 |
| Composite 2-col, uniqueness = 1.0 | +30 |
| Composite 3-col, uniqueness = 1.0 | +20 |

### Change Frequency
- FAST: >20% of values change between snapshots (or: amounts, balances, rates, status)
- SLOW: 1–20% change (or: names, address, type codes)
- STATIC: <1% change (or: IDs, DOB, SSN, open date)
- Heuristic only if no `_HIST`/`_SNAP`/`_HISTORY`/`_SNAPSHOT`/`_ARCHIVE` table found

---

## 8. Session State Keys (Streamlit)

| Key | Set by | Used by |
|-----|--------|---------|
| `sel_run_id` | Source Tables page | Profiling Review page pre-selection |
| `sel_table` | Source Tables page | Profiling Review page pre-selection |
| `ai_analysis_text` | Profiling Review AI tab | All AI parsing in same page |
| `ai_analysis_run` | Profiling Review AI tab | Freshness check (`ai_fresh`) |
| `ai_model_idx` | Profiling Review | Persists model selection across reruns |
| `ai_sensitivity_{run_id}` | Profiling Review | Column Statistics tab sensitivity colours |
| `ai_vault_notes` | Profiling Review | Design Workbench (_merge_vault_notes) |
| `ai_col_defs_{run_id}` | Profiling Review | Editable defs grid |
| `ai_tbl_desc_{run_id}` | Profiling Review | Table description input |
| `wb_workspace_id` | Design Workbench | Current workspace ID |
| `wb_workspace` | Design Workbench | Loaded workspace dict (None = needs loading) |
| `wb_source_key` | Design Workbench | `"{sys}__{table}"` format |
| `wb_flow` | Design Workbench | `'modeled'` or `'profiled'` |
| `wb_profiled_meta` | Design Workbench | Metadata for unmodeled profiled tables |
| `wb_source_cols` | Design Workbench | `[(col_name, dtype), ...]` for add-column dropdown |

Widget state keys prefixed `__wb_` (cleared by `_clear_wb_widget_state()` on workspace switch).

---

## 9. Deployment

### Local prerequisites
```
pip install snowflake-connector-python snowflake-snowpark-python python-dotenv
```

### `.env` file (project root)
```
SNOWFLAKE_ACCOUNT=<account_identifier>
SNOWFLAKE_USER=<username>
SNOWFLAKE_PASSWORD=<password>
```

### Deploy order (first time)
```bash
# Phase 1: Foundation
snowsql -f phase_1/01_foundation_ddl.sql
snowsql -f phase_1/02_seed_abbreviations.sql
snowsql -f phase_1/03_seed_system_prompt.sql
snowsql -f phase_1/04_seed_reference_hubs.sql

# Phase 2: SPs + Streamlit
python phase_2/deploy_sp.py
python phase_2/deploy_streamlit.py

# Phase 3: AI proposal SP + update Streamlit
python phase_3/deploy_phase3.py
```

### Streamlit app deployment (Snowflake)
- File uploaded to stage: `NEXUS_STREAMLIT_STAGE`
- App name: `NEXUS_DV2_APP`
- App is a single-file Streamlit in Snowflake app

### Update system prompt only
```bash
python phase_3/update_system_prompt.py
```

---

## 10. Key Design Decisions & Gotchas

### Cortex API
- `OBJECT_CONSTRUCT('max_tokens', 20000, 'temperature', 0)` must be a **literal**, not a bind param
- Messages passed via `PARSE_JSON(?)` bind param (avoids SQL injection / escaping issues with prompt content)
- `AI_COMPLETE(...)::VARCHAR` returns the content string directly (not a JSON envelope)

### Column definitions persistence
- `META.DV_COLUMN_DEFINITIONS` is the source of truth for approved definitions
- Survives session restarts, works for datashares and read-only sources where `ALTER COLUMN COMMENT` fails
- DDL generation overlays these definitions over the vault entity's stored `COLUMN_DEFINITION` for ATTR/BK roles
- Source column matching: `UPPER(cd.COLUMN_NAME) = UPPER(COALESCE(NULLIF(ec.SOURCE_COLUMN,''), ec.COLUMN_NAME))`

### Workspace JSON structure
```json
{
  "_meta": {proposal_id, source_table, source_system, source_schema, modeler_notes, run_id, input_scenario},
  "hubs": [...],
  "links": [...],
  "satellites": [...],
  "hash_definitions": [...],
  "warnings": [...],
  "_raw_ai_response": "..." (only if parse failed)
}
```

### Entity column roles
`HK` | `BK` | `FK_HK` | `HASHDIFF` | `META` | `ATTR` | `MAK`

### Source system badge
`_src_system_badge(system_code)` renders a grey pill — shared across all pages.

### Cascading selectors pattern
All pages use the pattern: Database → Schema → Table → (Run), loading options from Snowflake.  
The profiling review page pre-selects based on `st.session_state.sel_run_id`.

### `st.experimental_rerun()` usage
Used throughout for navigation (button triggers page transition). This is the Streamlit in Snowflake API — not deprecated in this environment.

### Wide tables (>60 cols)
`SP_GENERATE_DV_PROPOSAL` uses compact profiling format for wide tables to stay within Cortex token budget.

### Satellite naming (updated convention in SP)
`SAT_<SRCCODE>_<NOUN>_<DESCRIPTOR>` — source system code FIRST, single underscores throughout. The brief used double underscore `__` suffix but the SP instructions use single underscore. The system prompt section 20 uses `SAT_ACCTS_CUSTOMER_DETAILS` format (single underscore with source code prefix).

---

## 11. Build Status (as of 2026-04-13)

| Phase | Status | Notes |
|-------|--------|-------|
| Phase 1: Foundation DDL + Seeds | ✅ Complete | Deployed. Includes DV_COLUMN_DEFINITIONS table |
| Phase 2: Profiling SPs + Streamlit | ✅ Complete | SP_PROFILE_TABLE, SP_DETECT_PK_CANDIDATES, SP_DETECT_CHANGE_FREQUENCY deployed |
| Phase 3: AI Design Workbench | ✅ Complete | SP_GENERATE_DV_PROPOSAL deployed. Full UI in 04_streamlit_app.py |
| Phase 4: dbt generation | ✅ Complete (in-app) | Generate DBT page exists in app using automate_dv |
| Phase 5: Business Vault | 🔲 Not started | PIT, bridge tables — future work |

### Multi-model support for Design Raw Vault (2026-04-12)
`SP_GENERATE_DV_PROPOSAL` now accepts `AI_MODEL VARCHAR DEFAULT 'claude-opus-4-6'` as a 7th parameter. The Design Workbench page shows a horizontal radio above both panels (persisted in `wb_ai_model` / `wb_ai_model_idx` session state). All 4 SP call sites pass the selected model. Available models: claude-haiku-4-5, claude-sonnet-4-6, claude-opus-4-6, llama3.1-405b, openai-gpt-5.1. Deploy note: `deploy_phase3.py` now drops the old 6-arg SP signature before re-creating to avoid Snowflake overload ambiguity.

### Regenerate instruction weighting fix (2026-04-12)
Three bugs caused the Re-generate button to ignore modeler instructions:

1. **`04_streamlit_app.py` (Re-generate button, line ~2477):** New regen notes were appended to the END of old notes. Fixed: new instructions now go FIRST under a `CHANGE REQUEST (OVERRIDE — APPLY EXACTLY AS STATED):` label; old notes follow as secondary context.

2. **`01_sp_generate_dv_proposal.sql` (modeler notes rules, line ~294):** Rules only covered PKs, column defs, relationships — nothing about satellite/hub/link count or structural changes. Fixed: added explicit override rules for entity count ("create 3 satellites" must produce exactly 3) and CHANGE REQUEST labelled blocks.

3. **`01_sp_generate_dv_proposal.sql` (instruction #3, line ~322):** Satellite simplification guardrail ("consolidate where possible, avoid over-splitting") was unconditional and could override modeler intent. Fixed: added EXCEPTION clause — if MODELER NOTES specify a count, structure, or CHANGE REQUEST, the guardrail is fully ignored.

Deploy: `python phase_3/deploy_phase3.py` (SP) + `python phase_2/deploy_streamlit.py` (app).

### JSON parse resilience fix (2026-04-13)
`SP_GENERATE_DV_PROPOSAL` `_parse_ai_json` now has 5 passes (was 4):
- New Pass 4: `_sanitize_strings()` — walks the raw AI text and escapes literal control chars (`\n`, `\r`, `\t`, `<0x20`) found inside JSON string values. Fixes "Expecting ',' delimiter" errors caused by the AI embedding unescaped newlines in `column_definition` fields on large tables.
- Old truncation repair is now Pass 5.
- `_raw_ai_response` storage increased from 8000 → 20000 chars.

### Auto-recovery for parse-failed workspaces (2026-04-13)
When a user opens a workspace whose original AI generation failed to parse, the app now auto-regenerates silently on first open (shows a spinner). Controlled by `__wb_auto_regen_{ws_id}` session state flag — fires exactly once per workspace per session, cleared by `_clear_wb_widget_state()` on workspace switch. Falls back to manual Re-generate UI if the auto-attempt also fails.

### PII/PCI/SPI in AI Analysis — pending decision (2026-04-13)
The profiling AI analysis (`_build_ai_prompt`) sends `top_values` (up to 3 real cell values) and `min_value`/`max_value` per column to `AI_COMPLETE`. This is the main risk surface for sensitive data (SSN, credit card numbers, salaries, etc.). Snowflake Cortex processes within Snowflake's VPC — data does not reach Anthropic's API — but company policy may still prohibit sending sensitive values to any LLM.

Recommended approach (not yet implemented):
1. **Pattern-based auto-redaction (Option 1):** In `_build_ai_prompt()`, check each column name against a keyword list (SSN, CVV, PAN, IBAN, DOB, SALARY, PASSWORD, etc.) and replace `top_values`/`min_value`/`max_value` with `[REDACTED]` before building the prompt.
2. **Saved-sensitivity-driven redaction (Option 3):** For tables already analysed, load `DV_COLUMN_DEFINITIONS.IS_SENSITIVE` results and redact those columns automatically — more accurate than pattern matching.
3. Full column exclusion option (Option 4) — user can opt specific columns out of the AI prompt entirely.

---

## 12. Column Role Reference

| Role | Meaning | Nullable | Example |
|------|---------|----------|---------|
| HK | Hash Key — primary key of entity | NOT NULL | `CUSTOMER_HK BINARY(32)` |
| BK | Business Key — natural key from source | NOT NULL | `CUSTOMER_ID VARCHAR(50)` |
| FK_HK | Foreign Hash Key — references parent entity's HK | NOT NULL | `CUSTOMER_HK BINARY(32)` in a satellite |
| HASHDIFF | Hashdiff — hash of all ATTR columns for change detection | NOT NULL | `SAT_CUST_DTL_HASHDIFF BINARY(32)` |
| META | Metadata column | NOT NULL | `LOAD_DTS`, `REC_SRC`, `BATCH_ID` |
| ATTR | Descriptive attribute from source | NULL | `FIRST_NAME VARCHAR(100)` |
| MAK | Multi-Active Key — distinguishes rows within same snapshot | varies | `PHONE_TYPE_CD VARCHAR(20)` |

---

## 13. Files NOT to Modify Without Care

- `phase_1/01_foundation_ddl.sql` — foundation schema. Rerunning drops/recreates tables if using `CREATE OR REPLACE`. Use migrations instead.
- `phase_1/03_seed_system_prompt.sql` — system prompt. Changes affect all AI outputs. Deploy via `update_system_prompt.py`.
- `phase_2/04_streamlit_app.py` — the entire app is one file. It must remain a valid single-file Streamlit in Snowflake app.
- `.env` — contains credentials. Never commit.
