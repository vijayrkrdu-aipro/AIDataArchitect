-- ============================================================================
-- NEXUS DV2.0 — Phase 1: Foundation DDL
-- Creates the NEXUS database, all schemas, and all META registry tables.
-- Run this script first before any seed scripts.
-- ============================================================================

-- ============================================================================
-- STEP 1: DATABASE AND SCHEMAS
-- ============================================================================

CREATE DATABASE IF NOT EXISTS NEXUS
    COMMENT = 'NEXUS DV2.0 Automation Platform — enterprise banking data vault';

USE DATABASE NEXUS;

CREATE SCHEMA IF NOT EXISTS META
    COMMENT = 'Platform metadata and registry — all vault entity definitions, profiling, AI proposals';

CREATE SCHEMA IF NOT EXISTS STAGING
    COMMENT = 'Source data landing zone — staging tables loaded from source systems or files';

CREATE SCHEMA IF NOT EXISTS RAW_VAULT
    COMMENT = 'Generated Data Vault 2.0 hubs, links, and satellites';

CREATE SCHEMA IF NOT EXISTS BUSINESS_VAULT
    COMMENT = 'Future: PIT tables, bridge tables, computed satellites';

USE SCHEMA NEXUS.META;

-- ============================================================================
-- STEP 2: ABBREVIATION TABLE
-- Maintains logical-to-physical name mappings in sync with the Erwin .ABR file.
-- Used by the AI to decompose column names into meaningful terms.
-- ============================================================================

CREATE OR REPLACE TABLE META.DV_ABBREVIATION (
    ABBR_ID           NUMBER AUTOINCREMENT         NOT NULL,
    PHYSICAL_ABBR     VARCHAR(50)                  NOT NULL,   -- e.g. CUST
    LOGICAL_NAME      VARCHAR(200)                 NOT NULL,   -- e.g. Customer
    DOMAIN            VARCHAR(50),                             -- e.g. PARTY, ACCOUNT, FINANCE
    ERWIN_ABBR        VARCHAR(50),                             -- abbreviation as stored in Erwin .ABR file
    IS_ACTIVE         BOOLEAN                      DEFAULT TRUE,
    CREATED_DATE      TIMESTAMP_NTZ                DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_DV_ABBREVIATION PRIMARY KEY (ABBR_ID),
    CONSTRAINT UQ_PHYSICAL_ABBR   UNIQUE (PHYSICAL_ABBR)
);

COMMENT ON TABLE  META.DV_ABBREVIATION                IS 'Logical-to-physical abbreviation mapping. Kept in sync with Erwin .ABR file.';
COMMENT ON COLUMN META.DV_ABBREVIATION.PHYSICAL_ABBR  IS 'Short physical abbreviation used in column/table names (e.g. CUST)';
COMMENT ON COLUMN META.DV_ABBREVIATION.LOGICAL_NAME   IS 'Full logical name corresponding to the abbreviation (e.g. Customer)';
COMMENT ON COLUMN META.DV_ABBREVIATION.DOMAIN         IS 'Business domain grouping: PARTY, ACCOUNT, FINANCE, PRODUCT, REFERENCE';

-- ============================================================================
-- STEP 3: ENTITY REGISTRY
-- Central registry of all vault entities (hubs, links, satellites).
-- Only APPROVED entries are visible to the AI as context for new proposals.
-- ============================================================================

CREATE OR REPLACE TABLE META.DV_ENTITY (
    ENTITY_ID         VARCHAR(100)                 NOT NULL,   -- e.g. HUB_CUSTOMER
    ENTITY_TYPE       VARCHAR(10)                  NOT NULL,   -- HUB, LNK, SAT, MSAT, ESAT, PIT, BRG
    SCHEMA_NAME       VARCHAR(50)                  DEFAULT 'RAW_VAULT',
    LOGICAL_NAME      VARCHAR(200),
    DOMAIN            VARCHAR(50),
    SOURCE_SYSTEM     VARCHAR(50),                             -- null for HUBs/LNKs, populated for SATs
    PARENT_ENTITY_ID  VARCHAR(100),                            -- for SAT/ESAT: parent HUB or LNK entity_id
    APPROVAL_STATUS   VARCHAR(20)                  DEFAULT 'DRAFT',
    APPROVED_BY       VARCHAR(100),
    APPROVED_DATE     TIMESTAMP_NTZ,
    IS_ACTIVE         BOOLEAN                      DEFAULT TRUE,
    ERWIN_SYNC_STATUS VARCHAR(20)                  DEFAULT 'PENDING',  -- PENDING, SYNCED, CONFLICT
    CREATED_BY        VARCHAR(100)                 DEFAULT CURRENT_USER(),
    CREATED_DATE      TIMESTAMP_NTZ                DEFAULT CURRENT_TIMESTAMP(),
    LAST_MODIFIED     TIMESTAMP_NTZ                DEFAULT CURRENT_TIMESTAMP(),
    -- NOTE: Snowflake does not enforce CHECK constraints. Valid values enforced at application layer.
    -- ENTITY_TYPE: HUB, LNK, SAT, MSAT, ESAT, PIT, BRG
    -- APPROVAL_STATUS: DRAFT, APPROVED, DEPRECATED
    CONSTRAINT PK_DV_ENTITY PRIMARY KEY (ENTITY_ID)
);

COMMENT ON TABLE  META.DV_ENTITY                   IS 'Registry of all vault entities. Only APPROVED rows are used as AI context.';
COMMENT ON COLUMN META.DV_ENTITY.ENTITY_ID         IS 'Physical entity name (e.g. HUB_CUSTOMER, SAT_CUSTOMER_DETAILS__ACCT_SYS)';
COMMENT ON COLUMN META.DV_ENTITY.PARENT_ENTITY_ID  IS 'For satellites: the hub or link this satellite attaches to';
COMMENT ON COLUMN META.DV_ENTITY.SOURCE_SYSTEM     IS 'For source-specific satellites: the originating source system code';

-- ============================================================================
-- STEP 4: ENTITY COLUMNS
-- All columns for each registered entity.
-- ============================================================================

CREATE OR REPLACE TABLE META.DV_ENTITY_COLUMN (
    COLUMN_ID           NUMBER AUTOINCREMENT        NOT NULL,
    ENTITY_ID           VARCHAR(100)                NOT NULL,
    COLUMN_NAME         VARCHAR(100)                NOT NULL,   -- physical column name
    LOGICAL_NAME        VARCHAR(200),
    DATA_TYPE           VARCHAR(100)                NOT NULL,
    COLUMN_ROLE         VARCHAR(20)                 NOT NULL,   -- HK, BK, HASHDIFF, META, ATTR, FK_HK, MAK
    ORDINAL_POSITION    INT,
    IS_NULLABLE         BOOLEAN                     DEFAULT TRUE,
    COLUMN_DEFINITION   VARCHAR(2000),
    SOURCE_COLUMN       VARCHAR(2000),                          -- originating source column name (or hash description for HASHDIFF)
    CREATED_DATE        TIMESTAMP_NTZ               DEFAULT CURRENT_TIMESTAMP(),
    -- NOTE: Valid COLUMN_ROLE values enforced at application layer: HK, BK, HASHDIFF, META, ATTR, FK_HK, MAK
    CONSTRAINT PK_DV_ENTITY_COLUMN PRIMARY KEY (COLUMN_ID),
    CONSTRAINT UQ_ENTITY_COLUMN    UNIQUE (ENTITY_ID, COLUMN_NAME)
);

COMMENT ON TABLE  META.DV_ENTITY_COLUMN              IS 'Column definitions for each registered vault entity';
COMMENT ON COLUMN META.DV_ENTITY_COLUMN.COLUMN_ROLE  IS 'HK=Hash Key, BK=Business Key, FK_HK=Foreign Hash Key, MAK=Multi-Active Key, ATTR=Attribute, META=Metadata column';

-- ============================================================================
-- STEP 5: ENTITY RELATIONSHIPS
-- FK relationships between vault entities (hub→sat, hub→link, link→sat).
-- Stored explicitly because Snowflake FKs are non-enforced.
-- ============================================================================

CREATE OR REPLACE TABLE META.DV_RELATIONSHIP (
    RELATIONSHIP_ID   NUMBER AUTOINCREMENT          NOT NULL,
    FROM_ENTITY_ID    VARCHAR(100)                  NOT NULL,
    TO_ENTITY_ID      VARCHAR(100)                  NOT NULL,
    FROM_COLUMN       VARCHAR(100)                  NOT NULL,   -- the HK column in the child entity
    TO_COLUMN         VARCHAR(100)                  NOT NULL,   -- the HK column in the parent entity
    RELATIONSHIP_TYPE VARCHAR(50),                              -- HUB_TO_SAT, HUB_TO_LNK, LNK_TO_SAT
    CREATED_DATE      TIMESTAMP_NTZ                 DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_DV_RELATIONSHIP PRIMARY KEY (RELATIONSHIP_ID),
    CONSTRAINT UQ_RELATIONSHIP    UNIQUE (FROM_ENTITY_ID, TO_ENTITY_ID, FROM_COLUMN)
);

COMMENT ON TABLE META.DV_RELATIONSHIP IS 'Explicit FK relationships between vault entities. Required because Snowflake FKs are non-enforced.';

-- ============================================================================
-- STEP 6: HASH KEY DEFINITIONS
-- Records exactly how each hash key is computed for reproducibility.
-- ============================================================================

CREATE OR REPLACE TABLE META.DV_HASH_DEFINITION (
    HASH_DEF_ID       NUMBER AUTOINCREMENT          NOT NULL,
    ENTITY_ID         VARCHAR(100)                  NOT NULL,
    HASH_KEY_NAME     VARCHAR(100)                  NOT NULL,   -- e.g. CUSTOMER_HK
    HASH_TYPE         VARCHAR(20)                   NOT NULL,   -- BUSINESS_KEY, HASHDIFF
    SOURCE_COLUMNS    VARIANT                       NOT NULL,   -- JSON array of source columns in sort order
    NULL_REPLACEMENT  VARCHAR(50)                   DEFAULT '-1',
    DELIMITER         VARCHAR(10)                   DEFAULT '||',
    ALGORITHM         VARCHAR(20)                   DEFAULT 'SHA2_256',
    PREPROCESSING     VARCHAR(200)                  DEFAULT 'UPPER(TRIM(value))',
    CREATED_DATE      TIMESTAMP_NTZ                 DEFAULT CURRENT_TIMESTAMP(),
    -- NOTE: Valid HASH_TYPE values enforced at application layer: BUSINESS_KEY, HASHDIFF
    CONSTRAINT PK_DV_HASH_DEFINITION PRIMARY KEY (HASH_DEF_ID),
    CONSTRAINT UQ_HASH_KEY           UNIQUE (ENTITY_ID, HASH_KEY_NAME)
);

COMMENT ON TABLE  META.DV_HASH_DEFINITION              IS 'Hash key computation specifications. Ensures reproducible hashing across all load processes.';
COMMENT ON COLUMN META.DV_HASH_DEFINITION.SOURCE_COLUMNS IS 'JSON array of source column names, in the order used for concatenation before hashing';

-- ============================================================================
-- STEP 7: SOURCE-TO-VAULT COLUMN LINEAGE
-- Maps source columns to their target vault entity columns.
-- ============================================================================

CREATE OR REPLACE TABLE META.DV_SOURCE_MAPPING (
    MAPPING_ID        NUMBER AUTOINCREMENT          NOT NULL,
    SOURCE_SYSTEM     VARCHAR(50)                   NOT NULL,
    SOURCE_SCHEMA     VARCHAR(100),
    SOURCE_TABLE      VARCHAR(100)                  NOT NULL,
    SOURCE_COLUMN     VARCHAR(100)                  NOT NULL,
    TARGET_ENTITY_ID  VARCHAR(100)                  NOT NULL,
    TARGET_COLUMN     VARCHAR(100)                  NOT NULL,
    TRANSFORMATION    VARCHAR(500),                             -- optional SQL expression
    MAPPING_NOTES     VARCHAR(1000),
    IS_ACTIVE         BOOLEAN                       DEFAULT TRUE,
    CREATED_DATE      TIMESTAMP_NTZ                 DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_DV_SOURCE_MAPPING PRIMARY KEY (MAPPING_ID)
);

COMMENT ON TABLE META.DV_SOURCE_MAPPING IS 'Source-to-vault column lineage. Tracks how each source column maps to vault entity columns.';

-- ============================================================================
-- STEP 8: PROFILING RUN METADATA
-- One row per profiling execution. Tracks status and run parameters.
-- ============================================================================

CREATE OR REPLACE TABLE META.DV_PROFILING_RUN (
    RUN_ID            VARCHAR(200)                  NOT NULL,   -- ULID or UUID generated by SP
    SOURCE_SYSTEM     VARCHAR(50)                   NOT NULL,
    SOURCE_SCHEMA     VARCHAR(100)                  NOT NULL,
    SOURCE_TABLE      VARCHAR(100)                  NOT NULL,
    ROW_COUNT         NUMBER,
    COLUMN_COUNT      INT,
    PROFILING_METHOD  VARCHAR(10)                   DEFAULT 'EXACT',  -- EXACT or HLL
    STATUS            VARCHAR(20)                   DEFAULT 'RUNNING',
    STARTED_AT        TIMESTAMP_NTZ                 DEFAULT CURRENT_TIMESTAMP(),
    COMPLETED_AT      TIMESTAMP_NTZ,
    PROFILED_BY       VARCHAR(100)                  DEFAULT CURRENT_USER(),
    ERROR_MESSAGE     VARCHAR(2000),
    -- NOTE: Valid PROFILING_METHOD: EXACT, HLL. Valid STATUS: RUNNING, COMPLETED, FAILED. Enforced at application layer.
    CONSTRAINT PK_DV_PROFILING_RUN PRIMARY KEY (RUN_ID)
);

COMMENT ON TABLE META.DV_PROFILING_RUN IS 'Metadata for each profiling run. HLL method used automatically for tables with >10M rows.';

-- ============================================================================
-- STEP 9: PROFILING RESULTS
-- Per-column profiling statistics for each run.
-- ============================================================================

CREATE OR REPLACE TABLE META.DV_PROFILING_RESULTS (
    RESULT_ID           NUMBER AUTOINCREMENT         NOT NULL,
    RUN_ID              VARCHAR(200)                 NOT NULL,
    COLUMN_NAME         VARCHAR(100)                 NOT NULL,
    ORDINAL_POSITION    INT,
    SOURCE_DATA_TYPE    VARCHAR(100),                            -- as declared in source
    INFERRED_DATA_TYPE  VARCHAR(100),                            -- inferred via cast attempts
    ROW_COUNT           NUMBER,
    DISTINCT_COUNT      NUMBER,
    UNIQUENESS_RATIO    FLOAT,                                   -- DISTINCT_COUNT / ROW_COUNT
    NULL_COUNT          NUMBER,
    NULL_PERCENTAGE     FLOAT,
    MIN_LENGTH          INT,
    MAX_LENGTH          INT,
    AVG_LENGTH          FLOAT,
    MIN_VALUE           VARCHAR(500),
    MAX_VALUE           VARCHAR(500),
    TOP_VALUES          VARIANT,                                 -- JSON array of top 5 values
    PATTERN_DETECTED    VARCHAR(200),                            -- regex pattern if detected
    CHANGE_FREQUENCY    VARCHAR(10),                             -- FAST, SLOW, STATIC
    IS_PK_CANDIDATE     BOOLEAN                      DEFAULT FALSE,
    CREATED_DATE        TIMESTAMP_NTZ                DEFAULT CURRENT_TIMESTAMP(),
    -- NOTE: Valid CHANGE_FREQUENCY values enforced at application layer: FAST, SLOW, STATIC
    CONSTRAINT PK_DV_PROFILING_RESULTS PRIMARY KEY (RESULT_ID),
    CONSTRAINT UQ_RUN_COLUMN            UNIQUE (RUN_ID, COLUMN_NAME)
);

COMMENT ON TABLE META.DV_PROFILING_RESULTS IS 'Per-column profiling statistics. Feeds into PK candidate scoring and AI proposal generation.';

-- ============================================================================
-- STEP 10: PK CANDIDATES
-- Ranked primary key candidates (single and composite) detected by the profiler.
-- ============================================================================

CREATE OR REPLACE TABLE META.DV_PK_CANDIDATES (
    CANDIDATE_ID      NUMBER AUTOINCREMENT          NOT NULL,
    RUN_ID            VARCHAR(200)                  NOT NULL,
    SOURCE_TABLE      VARCHAR(100)                  NOT NULL,
    COLUMN_NAMES      VARIANT                       NOT NULL,   -- JSON array: ["CUST_ID"] or ["ACCT_ID","PROD_CD"]
    CANDIDATE_TYPE    VARCHAR(20),                              -- SINGLE, COMPOSITE
    PK_SCORE          INT                           NOT NULL,   -- scoring per spec §7
    SCORE_BREAKDOWN   VARIANT,                                  -- JSON with per-criterion scores
    MODELER_SELECTED  BOOLEAN                       DEFAULT FALSE,
    SELECTED_BY       VARCHAR(100),
    SELECTED_DATE     TIMESTAMP_NTZ,
    CREATED_DATE      TIMESTAMP_NTZ                 DEFAULT CURRENT_TIMESTAMP(),
    -- NOTE: Valid CANDIDATE_TYPE values enforced at application layer: SINGLE, COMPOSITE
    CONSTRAINT PK_DV_PK_CANDIDATES PRIMARY KEY (CANDIDATE_ID)
);

COMMENT ON TABLE  META.DV_PK_CANDIDATES             IS 'Ranked PK candidates per profiling run. Modeler selects the confirmed BK for AI input.';
COMMENT ON COLUMN META.DV_PK_CANDIDATES.PK_SCORE    IS 'Score 0-100: >=60 strong, 40-59 possible, <40 unlikely. See spec §7 for scoring criteria.';

-- ============================================================================
-- STEP 11: AI DESIGN PROPOSALS
-- Stores the raw AI-generated vault design for each source table request.
-- ============================================================================

CREATE OR REPLACE TABLE META.DV_DESIGN_PROPOSAL (
    PROPOSAL_ID       VARCHAR(200)                  NOT NULL,   -- ULID or UUID
    SOURCE_SYSTEM     VARCHAR(50)                   NOT NULL,
    SOURCE_SCHEMA     VARCHAR(100),
    SOURCE_TABLE      VARCHAR(100)                  NOT NULL,
    RUN_ID            VARCHAR(200),                             -- profiling run used, if any
    INPUT_SCENARIO    VARCHAR(20)                   NOT NULL,   -- FULL_PROFILING, METADATA_ONLY, COLUMN_NAMES_ONLY, DATA_INFERENCE
    AI_MODEL          VARCHAR(100)                  DEFAULT 'claude-opus-4-6',
    PROMPT_VERSION    VARCHAR(20),                              -- version of system prompt used
    PROPOSAL_JSON     VARIANT                       NOT NULL,   -- full structured AI response
    CONFIDENCE        VARCHAR(20),                              -- overall: HIGH, MEDIUM, LOW, INFERRED
    STATUS            VARCHAR(20)                   DEFAULT 'PENDING',
    GENERATED_BY      VARCHAR(100)                  DEFAULT CURRENT_USER(),
    GENERATED_AT      TIMESTAMP_NTZ                 DEFAULT CURRENT_TIMESTAMP(),
    -- NOTE: Valid values enforced at application layer:
    --   INPUT_SCENARIO: FULL_PROFILING, METADATA_ONLY, COLUMN_NAMES_ONLY, DATA_INFERENCE
    --   CONFIDENCE: HIGH, MEDIUM, LOW, INFERRED
    --   STATUS: PENDING, ACCEPTED, REJECTED, SUPERSEDED
    CONSTRAINT PK_DV_DESIGN_PROPOSAL PRIMARY KEY (PROPOSAL_ID)
);

COMMENT ON TABLE META.DV_DESIGN_PROPOSAL IS 'Raw AI-generated vault design proposals. One row per AI call. Status tracks lifecycle.';

-- ============================================================================
-- STEP 12: DESIGN WORKSPACE
-- The modeler''s working state — saves edits between sessions and tracks
-- versioning and approval status for multi-modeler conflict detection.
-- ============================================================================

CREATE OR REPLACE TABLE META.DV_DESIGN_WORKSPACE (
    WORKSPACE_ID      VARCHAR(200)                  NOT NULL,
    PROPOSAL_ID       VARCHAR(200),                             -- original AI proposal, if any
    SOURCE_TABLE      VARCHAR(100)                  NOT NULL,
    SOURCE_SYSTEM     VARCHAR(50)                   NOT NULL,
    SOURCE_SCHEMA     VARCHAR(100),
    WORKSPACE_JSON    VARIANT                       NOT NULL,   -- full editable model state
    STATUS            VARCHAR(20)                   DEFAULT 'DRAFT',
    VERSION_NUMBER    INT                           DEFAULT 1,
    PARENT_WORKSPACE  VARCHAR(200),                             -- previous version if re-opened after APPROVED
    AI_CONFIDENCE     VARCHAR(20),
    INPUT_SCENARIO    VARCHAR(20),
    LAST_MODIFIED_BY  VARCHAR(100)                  DEFAULT CURRENT_USER(),
    CREATED_BY        VARCHAR(100)                  DEFAULT CURRENT_USER(),
    CREATED_DATE      TIMESTAMP_NTZ                 DEFAULT CURRENT_TIMESTAMP(),
    LAST_MODIFIED     TIMESTAMP_NTZ                 DEFAULT CURRENT_TIMESTAMP(),
    -- NOTE: Valid values enforced at application layer:
    --   STATUS: DRAFT, IN_REVIEW, APPROVED, SUPERSEDED
    --   INPUT_SCENARIO: FULL_PROFILING, METADATA_ONLY, COLUMN_NAMES_ONLY, DATA_INFERENCE, MANUAL
    CONSTRAINT PK_DV_WORKSPACE PRIMARY KEY (WORKSPACE_ID)
);

COMMENT ON TABLE  META.DV_DESIGN_WORKSPACE               IS 'Modeler working state. Supports save/re-edit/versioning/approval. Used for conflict detection.';
COMMENT ON COLUMN META.DV_DESIGN_WORKSPACE.WORKSPACE_JSON IS 'Full model state as JSON: entities, columns, hash definitions, source mappings.';
COMMENT ON COLUMN META.DV_DESIGN_WORKSPACE.STATUS         IS 'DRAFT=work in progress, IN_REVIEW=submitted, APPROVED=written to registry, SUPERSEDED=replaced by new version';

-- NOTE: Snowflake does not support CREATE INDEX. Filtering on SOURCE_SYSTEM/SOURCE_TABLE/STATUS
-- is handled efficiently via Snowflake micro-partition pruning.

-- ============================================================================
-- STEP 13: AI SYSTEM PROMPT
-- Stores the DV2.0 standards as a versioned, sectioned system prompt.
-- Assembled at runtime by ordering on SECTION_ORDER and passed to AI_COMPLETE.
-- ============================================================================

CREATE OR REPLACE TABLE META.DV_AI_SYSTEM_PROMPT (
    PROMPT_ID         NUMBER AUTOINCREMENT          NOT NULL,
    SECTION_NAME      VARCHAR(50)                   NOT NULL,   -- ROLE, NAMING_CONVENTIONS, etc.
    SECTION_ORDER     INT                           NOT NULL,   -- assembly order for concatenation
    SECTION_CONTENT   VARCHAR(16000)                NOT NULL,
    IS_ACTIVE         BOOLEAN                       DEFAULT TRUE,
    VERSION           VARCHAR(20)                   DEFAULT '1.0',
    LAST_UPDATED_BY   VARCHAR(100)                  DEFAULT CURRENT_USER(),
    LAST_UPDATED      TIMESTAMP_NTZ                 DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_DV_AI_SYSTEM_PROMPT PRIMARY KEY (PROMPT_ID),
    CONSTRAINT UQ_SECTION_VERSION     UNIQUE (SECTION_NAME, VERSION)
);

COMMENT ON TABLE  META.DV_AI_SYSTEM_PROMPT                IS 'Versioned DV2.0 standards stored as sectioned system prompt. Editable by architecture team.';
COMMENT ON COLUMN META.DV_AI_SYSTEM_PROMPT.SECTION_ORDER  IS 'Controls the order sections are concatenated when assembling the prompt at runtime';

-- ============================================================================
-- STEP 14: AUDIT LOG
-- Immutable governance log for all platform actions.
-- ============================================================================

CREATE OR REPLACE TABLE META.DV_AUDIT_LOG (
    LOG_ID            NUMBER AUTOINCREMENT          NOT NULL,
    ACTION_TYPE       VARCHAR(50)                   NOT NULL,   -- PROFILE, GENERATE, SAVE, APPROVE, EXPORT, REJECT
    ENTITY_TYPE       VARCHAR(50),                              -- WORKSPACE, PROPOSAL, ENTITY, RUN
    ENTITY_ID         VARCHAR(200),                             -- the workspace_id, proposal_id, etc.
    SOURCE_TABLE      VARCHAR(100),
    SOURCE_SYSTEM     VARCHAR(50),
    ACTION_DETAILS    VARIANT,                                  -- JSON with action-specific metadata
    PERFORMED_BY      VARCHAR(100)                  DEFAULT CURRENT_USER(),
    PERFORMED_AT      TIMESTAMP_NTZ                 DEFAULT CURRENT_TIMESTAMP(),
    SESSION_ID        VARCHAR(200),
    CONSTRAINT PK_DV_AUDIT_LOG PRIMARY KEY (LOG_ID)
);

COMMENT ON TABLE META.DV_AUDIT_LOG IS 'Immutable audit trail for all platform actions. Used for governance, debugging, and conflict resolution.';

-- ============================================================================
-- VALIDATION — Confirm all tables created
-- ============================================================================

SELECT
    TABLE_NAME,
    COMMENT
FROM NEXUS.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'META'
ORDER BY TABLE_NAME;
