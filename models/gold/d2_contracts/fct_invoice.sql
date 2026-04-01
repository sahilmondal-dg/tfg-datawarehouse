-- =============================================================================
-- fct_invoice
-- Domain: D2 — Contracts & Revenue
-- Phase: 02 (Core Systems, Wks 4–15)
-- Source(s): NetSuite
-- Description: Canonical invoice fact. work_order_canonical_id resolves TRANSACTION.created_from_id — the NetSuite self-referencing FK linking an Invoice back to its originating Work Order.
-- Status: STUB — awaiting silver models and live data
-- Note: source_created_from_id preserves raw NetSuite transaction_id for traceability
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
