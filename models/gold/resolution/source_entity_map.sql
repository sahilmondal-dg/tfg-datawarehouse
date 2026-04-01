-- =============================================================================
-- source_entity_map
-- Domain: Resolution — Entity Resolution Backbone
-- Phase: 01 schema · Phase 02/03 population
-- Source(s): All source adapters reference this model
-- Description: Records how every source system identifier maps to a canonical ID. Resolution backbone — Jan-IT site IDs → NetSuite location IDs, ServiceChannel acct → NetSuite customer IDs, UtilizeCore location → NetSuite location IDs.
-- Status: STUB — schema defined Phase 01, population during Phase 02/03 build
-- Note: PK is map_id (not canonical_id). canonical_id here is a FK to the relevant canonical dim table and is NOT unique.
-- =============================================================================

{{
    config(
        materialized='table',
        schema='gold'
    )
}}

-- TODO: replace this stub with real logic once silver models are ready
-- Column list matches canonical DDL in tfg_canonical_ddl.sql
-- NOTE: canonical_id in this model is a FK to dim tables, not a surrogate PK.
--       The surrogate PK for this table is map_id.

select
    cast(null as varchar(64))     as canonical_id
