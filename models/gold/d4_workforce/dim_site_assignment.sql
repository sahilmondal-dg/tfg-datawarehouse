-- DO NOT POPULATE: No system of record for site assignments. Pending ADP scheduling adoption decision with Keaton/Zeel

-- =============================================================================
-- dim_site_assignment
-- Domain: D4 — Workforce
-- Phase: BLOCKED — ADP scheduling adoption decision required before populating
-- Source(s): ADP (scheduling module — not yet adopted)
-- Description: Stub only. No system of record for employee site assignments. Labour optimisation model depends on this — structural gap until ADP scheduling adopted.
-- Status: STUB — DO NOT POPULATE until adoption confirmed
-- =============================================================================

{{
    config(
        materialized='table',
        schema='gold'
    )
}}

-- TODO: replace this stub with real logic once ADP scheduling adoption is confirmed
-- Column list matches canonical DDL in tfg_canonical_ddl.sql

select
    cast(null as varchar(64))     as canonical_id
