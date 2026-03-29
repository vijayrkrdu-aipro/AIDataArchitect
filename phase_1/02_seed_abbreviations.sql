-- ============================================================================
-- NEXUS DV2.0 — Phase 1: Abbreviation Seeds
-- ~70 banking domain abbreviations, kept in sync with Erwin .ABR file.
-- Used by the AI to decompose physical column names into logical terms.
-- Run after 01_foundation_ddl.sql
-- ============================================================================

USE SCHEMA NEXUS.META;

INSERT INTO META.DV_ABBREVIATION (PHYSICAL_ABBR, LOGICAL_NAME, DOMAIN, ERWIN_ABBR)
VALUES

-- ── PARTY DOMAIN ──────────────────────────────────────────────────────────
('CUST',    'Customer',                     'PARTY',     'CUST'),
('CLNT',    'Client',                       'PARTY',     'CLNT'),
('PRTY',    'Party',                        'PARTY',     'PRTY'),
('PRSN',    'Person',                       'PARTY',     'PRSN'),
('EMPL',    'Employee',                     'PARTY',     'EMPL'),
('VNDR',    'Vendor',                       'PARTY',     'VNDR'),
('BRWR',    'Borrower',                     'PARTY',     'BRWR'),
('BENE',    'Beneficiary',                  'PARTY',     'BENE'),
('CNTR',    'Counterparty',                 'PARTY',     'CNTR'),
('AGNT',    'Agent',                        'PARTY',     'AGNT'),

-- ── ACCOUNT DOMAIN ────────────────────────────────────────────────────────
('ACCT',    'Account',                      'ACCOUNT',   'ACCT'),
('DPST',    'Deposit',                      'ACCOUNT',   'DPST'),
('OVRDR',   'Overdraft',                    'ACCOUNT',   'OVRDR'),
('LDGR',    'Ledger',                       'ACCOUNT',   'LDGR'),
('GL',      'General Ledger',               'ACCOUNT',   'GL'),
('SUBL',    'Sub-Ledger',                   'ACCOUNT',   'SUBL'),
('PORT',    'Portfolio',                    'ACCOUNT',   'PORT'),
('SUBSID',  'Subsidiary',                   'ACCOUNT',   'SUBSID'),

-- ── FINANCE / TRANSACTION DOMAIN ──────────────────────────────────────────
('TXN',     'Transaction',                  'FINANCE',   'TXN'),
('TRN',     'Transaction',                  'FINANCE',   'TRN'),
('PYMT',    'Payment',                      'FINANCE',   'PYMT'),
('AMT',     'Amount',                       'FINANCE',   'AMT'),
('BAL',     'Balance',                      'FINANCE',   'BAL'),
('INTRST',  'Interest',                     'FINANCE',   'INTRST'),
('RATE',    'Rate',                         'FINANCE',   'RATE'),
('MRGN',    'Margin',                       'FINANCE',   'MRGN'),
('FEE',     'Fee',                          'FINANCE',   'FEE'),
('CHRG',    'Charge',                       'FINANCE',   'CHRG'),
('XFER',    'Transfer',                     'FINANCE',   'XFER'),
('CRDTL',   'Credit Limit',                 'FINANCE',   'CRDTL'),
('LMT',     'Limit',                        'FINANCE',   'LMT'),
('EXCH',    'Exchange',                     'FINANCE',   'EXCH'),

-- ── LOAN / CREDIT DOMAIN ──────────────────────────────────────────────────
('LN',      'Loan',                         'CREDIT',    'LN'),
('MTG',     'Mortgage',                     'CREDIT',    'MTG'),
('COLL',    'Collateral',                   'CREDIT',    'COLL'),
('INVST',   'Investment',                   'CREDIT',    'INVST'),
('FACLT',   'Facility',                     'CREDIT',    'FACLT'),
('EXPSR',   'Exposure',                     'CREDIT',    'EXPSR'),

-- ── PRODUCT DOMAIN ────────────────────────────────────────────────────────
('PROD',    'Product',                      'PRODUCT',   'PROD'),
('PRDCT',   'Product',                      'PRODUCT',   'PRDCT'),
('SRVC',    'Service',                      'PRODUCT',   'SRVC'),
('OFFR',    'Offer',                        'PRODUCT',   'OFFR'),
('CNTRCT',  'Contract',                     'PRODUCT',   'CNTRCT'),
('AGRMT',   'Agreement',                    'PRODUCT',   'AGRMT'),
('BNDLE',   'Bundle',                       'PRODUCT',   'BNDLE'),

-- ── REFERENCE / CODE DOMAIN ───────────────────────────────────────────────
('CURR',    'Currency',                     'REFERENCE', 'CURR'),
('CNTRY',   'Country',                      'REFERENCE', 'CNTRY'),
('CTRY',    'Country',                      'REFERENCE', 'CTRY'),
('BR',      'Branch',                       'REFERENCE', 'BR'),
('BRN',     'Branch',                       'REFERENCE', 'BRN'),
('INST',    'Institution',                  'REFERENCE', 'INST'),
('RGN',     'Region',                       'REFERENCE', 'RGN'),
('DEPT',    'Department',                   'REFERENCE', 'DEPT'),
('GRP',     'Group',                        'REFERENCE', 'GRP'),
('CD',      'Code',                         'REFERENCE', 'CD'),
('TYP',     'Type',                         'REFERENCE', 'TYP'),
('STAT',    'Status',                       'REFERENCE', 'STAT'),
('KYC',     'Know Your Customer',           'REFERENCE', 'KYC'),

-- ── GENERIC / COMMON ──────────────────────────────────────────────────────
('ID',      'Identifier',                   NULL,        'ID'),
('NBR',     'Number',                       NULL,        'NBR'),
('NUM',     'Number',                       NULL,        'NUM'),
('NM',      'Name',                         NULL,        'NM'),
('FRST',    'First',                        NULL,        'FRST'),
('LST',     'Last',                         NULL,        'LST'),
('MDL',     'Middle',                       NULL,        'MDL'),
('ADDR',    'Address',                      NULL,        'ADDR'),
('EMAIL',   'Email Address',                NULL,        'EMAIL'),
('PHNE',    'Phone',                        NULL,        'PHNE'),
('DT',      'Date',                         NULL,        'DT'),
('DTS',     'Date Timestamp',               NULL,        'DTS'),
('YR',      'Year',                         NULL,        'YR'),
('EFF',     'Effective',                    NULL,        'EFF'),
('EXPRY',   'Expiry',                       NULL,        'EXPRY'),
('DESCR',   'Description',                  NULL,        'DESCR'),
('DEFN',    'Definition',                   NULL,        'DEFN'),
('FLG',     'Flag',                         NULL,        'FLG'),
('REF',     'Reference',                    NULL,        'REF'),
('SRC',     'Source',                       NULL,        'SRC'),
('SYS',     'System',                       NULL,        'SYS'),
('REC',     'Record',                       NULL,        'REC'),
('ORG',     'Organization',                 NULL,        'ORG'),
('BSN',     'Business',                     NULL,        'BSN'),
('MGMT',    'Management',                   NULL,        'MGMT'),
('TIN',     'Tax Identification Number',    NULL,        'TIN'),
('SSN',     'Social Security Number',       NULL,        'SSN'),
('RLS',     'Relationship',                 NULL,        'RLS')
;

-- ── VALIDATION ────────────────────────────────────────────────────────────
SELECT
    DOMAIN,
    COUNT(*) AS ABBR_COUNT
FROM META.DV_ABBREVIATION
GROUP BY DOMAIN
ORDER BY ABBR_COUNT DESC;
