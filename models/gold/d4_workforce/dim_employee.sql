-- =============================================================================
-- dim_employee
-- Domain: D4 — Workforce
-- Phase: 02 (Core Systems, Wks 4–15)
-- Source(s): ADP only (WinTeam removed from scope entirely)
-- Description: Canonical employee dimension. ADP is authoritative — four matching signals (first_name, last_name, hire_date, email) used for entity resolution against other systems.
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
