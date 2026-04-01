-- =============================================================================
-- fct_labour_entry
-- Domain: D4 — Workforce
-- Phase: 02 (Core Systems, Wks 4–15)
-- Source(s): ADP only
-- Description: Canonical labour entry fact. Captures hours worked vs hours billed per employee — critical for pay-by-hour contract margin management.
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
