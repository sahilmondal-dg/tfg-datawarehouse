-- =============================================================================
-- fct_contract
-- Domain: D2 — Contracts & Revenue
-- Phase: 02 (Core Systems, Wks 4–15)
-- Source(s): NetSuite
-- Description: Reconstructed contract entity. NetSuite has no native contracts table — contract_ref is a free-text field on the customer record.
-- Status: STUB — awaiting silver models and live data
-- Note: contract_type CHECK ('fixed-fee', 'pay-by-hour', 'isp') drives labour optimisation logic
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
