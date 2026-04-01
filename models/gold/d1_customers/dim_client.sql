-- =============================================================================
-- dim_client
-- Domain: D1 — Customers & Locations
-- Phase: 02 (Core Systems, Wks 4–15)
-- Source(s): NetSuite (master), Jan-IT (Phase 02), ServiceChannel/UtilizeCore (Phase 03)
-- Description: Canonical client dimension. Deduplicates customers across all source systems.
-- Status: STUB — awaiting silver models and live data
-- =============================================================================

{{
    config(
        materialized='table',
        schema='gold'
    )
}}

-- TODO: replace this stub with real logic once silver models are ready
-- Column list matches canonical DDL in tfg_canonical_ddl.sql

select
    cast(null as varchar(64))     as canonical_id
