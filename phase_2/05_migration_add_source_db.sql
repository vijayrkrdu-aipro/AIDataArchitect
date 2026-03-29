-- ============================================================================
-- NEXUS DV2.0 — Phase 2 Migration: Add SOURCE_DATABASE
-- Run this if Phase 1 is already deployed and you need to support
-- profiling tables from any database (not just NEXUS).
-- Safe to run multiple times (uses IF NOT EXISTS pattern via TRY).
-- ============================================================================

USE SCHEMA NEXUS.META;

-- Add SOURCE_DATABASE to the profiling run table
ALTER TABLE META.DV_PROFILING_RUN
    ADD COLUMN IF NOT EXISTS SOURCE_DATABASE VARCHAR(100);

-- Add SOURCE_DATABASE to source mapping for full lineage tracking
ALTER TABLE META.DV_SOURCE_MAPPING
    ADD COLUMN IF NOT EXISTS SOURCE_DATABASE VARCHAR(100);

-- Backfill existing rows with NEXUS (assumed origin before this migration)
UPDATE META.DV_PROFILING_RUN
SET SOURCE_DATABASE = 'NEXUS'
WHERE SOURCE_DATABASE IS NULL;

UPDATE META.DV_SOURCE_MAPPING
SET SOURCE_DATABASE = 'NEXUS'
WHERE SOURCE_DATABASE IS NULL;

-- Validation
SELECT 'DV_PROFILING_RUN'  AS TABLE_NAME, COUNT(*) AS ROWS, COUNT(SOURCE_DATABASE) AS DB_SET FROM META.DV_PROFILING_RUN
UNION ALL
SELECT 'DV_SOURCE_MAPPING', COUNT(*),             COUNT(SOURCE_DATABASE)             FROM META.DV_SOURCE_MAPPING;
