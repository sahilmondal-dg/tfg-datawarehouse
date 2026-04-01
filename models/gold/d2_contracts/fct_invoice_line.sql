-- =============================================================================
-- fct_invoice_line
-- Domain: D2 — Contracts & Revenue
-- Phase: 02 (Core Systems, Wks 4–15)
-- Source(s): NetSuite
-- Description: Line-level detail for canonical invoices. GL account and department dimensions preserved for downstream financial analysis.
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
