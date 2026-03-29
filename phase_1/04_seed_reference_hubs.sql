-- ============================================================================
-- NEXUS DV2.0 — Phase 1: Reference Hub DDL + Seed Data
-- Creates the four pre-seeded reference hubs in RAW_VAULT schema:
--   HUB_CURRENCY, HUB_COUNTRY, HUB_BRANCH, HUB_GL_ACCOUNT
-- These are registered in META and physically created in RAW_VAULT.
-- The AI treats these as existing hubs and will NOT create new ones for these.
-- Run after 01_foundation_ddl.sql
-- ============================================================================

USE DATABASE NEXUS;

-- ============================================================================
-- STEP 1: PHYSICAL TABLE DDL — Reference Hubs in RAW_VAULT
-- ============================================================================

-- ── HUB_CURRENCY ──────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE RAW_VAULT.HUB_CURRENCY (
    CURRENCY_HK     BINARY(32)      NOT NULL,   -- SHA2_BINARY(256) of CURR_CD
    CURR_CD         VARCHAR(3)      NOT NULL,   -- ISO 4217 currency code (e.g. USD, GBP)
    LOAD_DTS        TIMESTAMP_NTZ   NOT NULL    DEFAULT CURRENT_TIMESTAMP(),
    REC_SRC         VARCHAR(100)    NOT NULL,
    CONSTRAINT PK_HUB_CURRENCY PRIMARY KEY (CURRENCY_HK)
);

COMMENT ON TABLE  RAW_VAULT.HUB_CURRENCY          IS 'Reference Hub: Currency. Business key is ISO 4217 currency code.';
COMMENT ON COLUMN RAW_VAULT.HUB_CURRENCY.CURR_CD  IS 'ISO 4217 3-character currency code (e.g. USD, GBP, EUR, SGD)';

-- ── HUB_COUNTRY ───────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE RAW_VAULT.HUB_COUNTRY (
    COUNTRY_HK      BINARY(32)      NOT NULL,   -- SHA2_BINARY(256) of CNTRY_CD
    CNTRY_CD        VARCHAR(3)      NOT NULL,   -- ISO 3166-1 alpha-2 or alpha-3 code
    LOAD_DTS        TIMESTAMP_NTZ   NOT NULL    DEFAULT CURRENT_TIMESTAMP(),
    REC_SRC         VARCHAR(100)    NOT NULL,
    CONSTRAINT PK_HUB_COUNTRY PRIMARY KEY (COUNTRY_HK)
);

COMMENT ON TABLE  RAW_VAULT.HUB_COUNTRY           IS 'Reference Hub: Country. Business key is ISO 3166-1 country code.';
COMMENT ON COLUMN RAW_VAULT.HUB_COUNTRY.CNTRY_CD  IS 'ISO 3166-1 country code (e.g. US, GB, AU — use 2-char alpha-2)';

-- ── HUB_BRANCH ────────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE RAW_VAULT.HUB_BRANCH (
    BRANCH_HK       BINARY(32)      NOT NULL,   -- SHA2_BINARY(256) of BRN_CD
    BRN_CD          VARCHAR(20)     NOT NULL,   -- internal branch code
    LOAD_DTS        TIMESTAMP_NTZ   NOT NULL    DEFAULT CURRENT_TIMESTAMP(),
    REC_SRC         VARCHAR(100)    NOT NULL,
    CONSTRAINT PK_HUB_BRANCH PRIMARY KEY (BRANCH_HK)
);

COMMENT ON TABLE  RAW_VAULT.HUB_BRANCH            IS 'Reference Hub: Branch. Business key is internal branch code.';
COMMENT ON COLUMN RAW_VAULT.HUB_BRANCH.BRN_CD     IS 'Internal branch identifier code used across source systems';

-- ── HUB_GL_ACCOUNT ────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE RAW_VAULT.HUB_GL_ACCOUNT (
    GL_ACCOUNT_HK   BINARY(32)      NOT NULL,   -- SHA2_BINARY(256) of GL_ACCT_NBR
    GL_ACCT_NBR     VARCHAR(20)     NOT NULL,   -- general ledger account number
    LOAD_DTS        TIMESTAMP_NTZ   NOT NULL    DEFAULT CURRENT_TIMESTAMP(),
    REC_SRC         VARCHAR(100)    NOT NULL,
    CONSTRAINT PK_HUB_GL_ACCOUNT PRIMARY KEY (GL_ACCOUNT_HK)
);

COMMENT ON TABLE  RAW_VAULT.HUB_GL_ACCOUNT              IS 'Reference Hub: General Ledger Account. Business key is GL account number.';
COMMENT ON COLUMN RAW_VAULT.HUB_GL_ACCOUNT.GL_ACCT_NBR  IS 'General Ledger account number as used in the core banking chart of accounts';

-- ============================================================================
-- STEP 2: REGISTER ALL REFERENCE HUBS IN META.DV_ENTITY
-- ============================================================================

INSERT INTO META.DV_ENTITY
    (ENTITY_ID, ENTITY_TYPE, SCHEMA_NAME, LOGICAL_NAME, DOMAIN,
     APPROVAL_STATUS, APPROVED_BY, APPROVED_DATE, CREATED_BY)
VALUES
    ('HUB_CURRENCY',   'HUB', 'RAW_VAULT', 'Currency Hub',             'REFERENCE',
     'APPROVED', 'SYSTEM', CURRENT_TIMESTAMP(), 'SYSTEM'),

    ('HUB_COUNTRY',    'HUB', 'RAW_VAULT', 'Country Hub',              'REFERENCE',
     'APPROVED', 'SYSTEM', CURRENT_TIMESTAMP(), 'SYSTEM'),

    ('HUB_BRANCH',     'HUB', 'RAW_VAULT', 'Branch Hub',               'REFERENCE',
     'APPROVED', 'SYSTEM', CURRENT_TIMESTAMP(), 'SYSTEM'),

    ('HUB_GL_ACCOUNT', 'HUB', 'RAW_VAULT', 'General Ledger Account Hub', 'FINANCE',
     'APPROVED', 'SYSTEM', CURRENT_TIMESTAMP(), 'SYSTEM')
;

-- ============================================================================
-- STEP 3: REGISTER COLUMNS FOR EACH REFERENCE HUB
-- ============================================================================

-- HUB_CURRENCY columns
INSERT INTO META.DV_ENTITY_COLUMN
    (ENTITY_ID, COLUMN_NAME, LOGICAL_NAME, DATA_TYPE, COLUMN_ROLE, ORDINAL_POSITION, IS_NULLABLE, COLUMN_DEFINITION)
VALUES
    ('HUB_CURRENCY', 'CURRENCY_HK', 'Currency Hash Key',   'BINARY(32)',    'HK',   1, FALSE, 'SHA2_BINARY(256) of CURR_CD. Primary key.'),
    ('HUB_CURRENCY', 'CURR_CD',     'Currency Code',        'VARCHAR(3)',    'BK',   2, FALSE, 'ISO 4217 3-character currency code (e.g. USD, GBP, EUR).'),
    ('HUB_CURRENCY', 'LOAD_DTS',    'Load Date Timestamp',  'TIMESTAMP_NTZ','META', 3, FALSE, 'Timestamp when the record was loaded into the vault.'),
    ('HUB_CURRENCY', 'REC_SRC',     'Record Source',        'VARCHAR(100)', 'META', 4, FALSE, 'Identifier of the source system that provided this record.')
;

-- HUB_COUNTRY columns
INSERT INTO META.DV_ENTITY_COLUMN
    (ENTITY_ID, COLUMN_NAME, LOGICAL_NAME, DATA_TYPE, COLUMN_ROLE, ORDINAL_POSITION, IS_NULLABLE, COLUMN_DEFINITION)
VALUES
    ('HUB_COUNTRY', 'COUNTRY_HK', 'Country Hash Key',    'BINARY(32)',    'HK',   1, FALSE, 'SHA2_BINARY(256) of CNTRY_CD. Primary key.'),
    ('HUB_COUNTRY', 'CNTRY_CD',   'Country Code',         'VARCHAR(3)',    'BK',   2, FALSE, 'ISO 3166-1 alpha-2 country code (e.g. US, GB, AU).'),
    ('HUB_COUNTRY', 'LOAD_DTS',   'Load Date Timestamp',  'TIMESTAMP_NTZ','META', 3, FALSE, 'Timestamp when the record was loaded into the vault.'),
    ('HUB_COUNTRY', 'REC_SRC',    'Record Source',        'VARCHAR(100)', 'META', 4, FALSE, 'Identifier of the source system that provided this record.')
;

-- HUB_BRANCH columns
INSERT INTO META.DV_ENTITY_COLUMN
    (ENTITY_ID, COLUMN_NAME, LOGICAL_NAME, DATA_TYPE, COLUMN_ROLE, ORDINAL_POSITION, IS_NULLABLE, COLUMN_DEFINITION)
VALUES
    ('HUB_BRANCH', 'BRANCH_HK', 'Branch Hash Key',      'BINARY(32)',    'HK',   1, FALSE, 'SHA2_BINARY(256) of BRN_CD. Primary key.'),
    ('HUB_BRANCH', 'BRN_CD',    'Branch Code',           'VARCHAR(20)',   'BK',   2, FALSE, 'Internal branch code as used across source systems.'),
    ('HUB_BRANCH', 'LOAD_DTS',  'Load Date Timestamp',   'TIMESTAMP_NTZ','META', 3, FALSE, 'Timestamp when the record was loaded into the vault.'),
    ('HUB_BRANCH', 'REC_SRC',   'Record Source',         'VARCHAR(100)', 'META', 4, FALSE, 'Identifier of the source system that provided this record.')
;

-- HUB_GL_ACCOUNT columns
INSERT INTO META.DV_ENTITY_COLUMN
    (ENTITY_ID, COLUMN_NAME, LOGICAL_NAME, DATA_TYPE, COLUMN_ROLE, ORDINAL_POSITION, IS_NULLABLE, COLUMN_DEFINITION)
VALUES
    ('HUB_GL_ACCOUNT', 'GL_ACCOUNT_HK', 'GL Account Hash Key',   'BINARY(32)',    'HK',   1, FALSE, 'SHA2_BINARY(256) of GL_ACCT_NBR. Primary key.'),
    ('HUB_GL_ACCOUNT', 'GL_ACCT_NBR',   'GL Account Number',      'VARCHAR(20)',   'BK',   2, FALSE, 'General Ledger account number from the chart of accounts.'),
    ('HUB_GL_ACCOUNT', 'LOAD_DTS',      'Load Date Timestamp',    'TIMESTAMP_NTZ','META', 3, FALSE, 'Timestamp when the record was loaded into the vault.'),
    ('HUB_GL_ACCOUNT', 'REC_SRC',       'Record Source',          'VARCHAR(100)', 'META', 4, FALSE, 'Identifier of the source system that provided this record.')
;

-- ============================================================================
-- STEP 4: REGISTER HASH DEFINITIONS FOR REFERENCE HUBS
-- ============================================================================

INSERT INTO META.DV_HASH_DEFINITION
    (ENTITY_ID, HASH_KEY_NAME, HASH_TYPE, SOURCE_COLUMNS, NULL_REPLACEMENT, DELIMITER, ALGORITHM, PREPROCESSING)
VALUES
    ('HUB_CURRENCY',   'CURRENCY_HK',   'BUSINESS_KEY', PARSE_JSON('["CURR_CD"]'),      '-1', '||', 'SHA2_256', 'UPPER(TRIM(value))'),
    ('HUB_COUNTRY',    'COUNTRY_HK',    'BUSINESS_KEY', PARSE_JSON('["CNTRY_CD"]'),     '-1', '||', 'SHA2_256', 'UPPER(TRIM(value))'),
    ('HUB_BRANCH',     'BRANCH_HK',     'BUSINESS_KEY', PARSE_JSON('["BRN_CD"]'),       '-1', '||', 'SHA2_256', 'UPPER(TRIM(value))'),
    ('HUB_GL_ACCOUNT', 'GL_ACCOUNT_HK', 'BUSINESS_KEY', PARSE_JSON('["GL_ACCT_NBR"]'), '-1', '||', 'SHA2_256', 'UPPER(TRIM(value))')
;

-- ============================================================================
-- STEP 5: SEED DATA — Common reference values
-- ============================================================================

-- Seed HUB_CURRENCY with major banking currencies
INSERT INTO RAW_VAULT.HUB_CURRENCY (CURRENCY_HK, CURR_CD, REC_SRC)
SELECT SHA2_BINARY(UPPER(TRIM(CURR_CD)), 256), CURR_CD, 'NEXUS_SEED'
FROM (
    VALUES
    ('USD'), ('GBP'), ('EUR'), ('AUD'), ('CAD'), ('CHF'), ('JPY'),
    ('SGD'), ('HKD'), ('NZD'), ('NOK'), ('SEK'), ('DKK'), ('ZAR'),
    ('INR'), ('CNY'), ('AED'), ('SAR'), ('MYR'), ('THB')
) AS T(CURR_CD);

-- Seed HUB_COUNTRY with major banking jurisdictions
INSERT INTO RAW_VAULT.HUB_COUNTRY (COUNTRY_HK, CNTRY_CD, REC_SRC)
SELECT SHA2_BINARY(UPPER(TRIM(CNTRY_CD)), 256), CNTRY_CD, 'NEXUS_SEED'
FROM (
    VALUES
    ('US'), ('GB'), ('AU'), ('CA'), ('DE'), ('FR'), ('JP'),
    ('SG'), ('HK'), ('NZ'), ('NO'), ('SE'), ('DK'), ('ZA'),
    ('IN'), ('CN'), ('AE'), ('SA'), ('MY'), ('TH'), ('CH'),
    ('NL'), ('BE'), ('IT'), ('ES'), ('IE'), ('LU'), ('BR'), ('MX')
) AS T(CNTRY_CD);

-- ============================================================================
-- STEP 6: AUDIT LOG ENTRY
-- ============================================================================

INSERT INTO META.DV_AUDIT_LOG
    (ACTION_TYPE, ENTITY_TYPE, ENTITY_ID, ACTION_DETAILS, PERFORMED_BY)
VALUES
    ('SEED', 'ENTITY', 'HUB_CURRENCY,HUB_COUNTRY,HUB_BRANCH,HUB_GL_ACCOUNT',
     PARSE_JSON('{"action": "Phase 1 reference hub seed", "hub_count": 4, "currency_rows": 20, "country_rows": 29}'),
     CURRENT_USER())
;

-- ============================================================================
-- VALIDATION
-- ============================================================================

-- Confirm all 4 hubs are registered and approved
SELECT
    ENTITY_ID,
    ENTITY_TYPE,
    DOMAIN,
    APPROVAL_STATUS
FROM META.DV_ENTITY
WHERE APPROVAL_STATUS = 'APPROVED'
ORDER BY ENTITY_ID;

-- Confirm seed row counts
SELECT 'HUB_CURRENCY' AS HUB, COUNT(*) AS ROW_COUNT FROM RAW_VAULT.HUB_CURRENCY
UNION ALL
SELECT 'HUB_COUNTRY',          COUNT(*)               FROM RAW_VAULT.HUB_COUNTRY
UNION ALL
SELECT 'HUB_BRANCH',           COUNT(*)               FROM RAW_VAULT.HUB_BRANCH
UNION ALL
SELECT 'HUB_GL_ACCOUNT',       COUNT(*)               FROM RAW_VAULT.HUB_GL_ACCOUNT
ORDER BY HUB;
