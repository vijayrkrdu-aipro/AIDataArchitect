-- ============================================================================
-- NEXUS DV2.0 — Phase 1 Addendum
-- Add DV_DESIGN_WORKSPACE table for Phase 3 save/re-edit functionality
-- Run this AFTER phase_1_registry_ddl.sql
-- ============================================================================

USE SCHEMA NEXUS.META;

CREATE OR REPLACE TABLE META.DV_DESIGN_WORKSPACE (
    WORKSPACE_ID      VARCHAR(200)    NOT NULL,
    PROPOSAL_ID       VARCHAR(200),
    SOURCE_TABLE      VARCHAR(100)    NOT NULL,
    SOURCE_SYSTEM     VARCHAR(50)     NOT NULL,
    SOURCE_SCHEMA     VARCHAR(100),
    WORKSPACE_JSON    VARIANT         NOT NULL,
    STATUS            VARCHAR(20)     DEFAULT 'DRAFT',
    VERSION_NUMBER    INT             DEFAULT 1,
    PARENT_WORKSPACE  VARCHAR(200),
    AI_CONFIDENCE     VARCHAR(20),
    INPUT_SCENARIO    VARCHAR(20),
    LAST_MODIFIED_BY  VARCHAR(100)    DEFAULT CURRENT_USER(),
    CREATED_BY        VARCHAR(100)    DEFAULT CURRENT_USER(),
    CREATED_DATE      TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    LAST_MODIFIED     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    -- NOTE: Snowflake does not support CHECK constraints. Valid values enforced at application layer.
    --   STATUS: DRAFT, IN_REVIEW, APPROVED, SUPERSEDED
    --   INPUT_SCENARIO: FULL_PROFILING, METADATA_ONLY, COLUMN_NAMES_ONLY, DATA_INFERENCE, MANUAL
    CONSTRAINT PK_DV_WORKSPACE PRIMARY KEY (WORKSPACE_ID)
);

COMMENT ON TABLE META.DV_DESIGN_WORKSPACE IS 
    'Modeler working state for DV2.0 design. Supports save, re-edit, versioning, and approval workflow.';

-- NOTE: Snowflake does not support CREATE INDEX. Filtering on SOURCE_SYSTEM/SOURCE_TABLE/STATUS
-- is handled efficiently via Snowflake micro-partition pruning.

-- Validation
SELECT TABLE_NAME, COMMENT 
FROM NEXUS.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'META' AND TABLE_NAME = 'DV_DESIGN_WORKSPACE';
