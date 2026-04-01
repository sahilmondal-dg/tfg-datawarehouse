-- CONDITIONAL (EBS): Phase 04 only. Do not build EBS adapter until Oracle EBS migration confirmed (target Sep/Oct). EBS rows are historical only — one-time extract, NOT a live pipeline.

-- =============================================================================
-- fct_gl_entry
-- Domain: D5 — Financials
-- Phase: 02 NetSuite (Core Systems, Wks 4–15) · Phase 04 EBS CONDITIONAL (Post Q3/Q4)
-- Source(s): NetSuite (live GL), Oracle EBS (one-time historical extract — NOT live)
-- Description: Event-based canonical GL entry. Atomic unit is a business event, not an ERP record. Each source has its own per-ERP adapter staging model.
-- Status: STUB — awaiting silver models and live data
-- Note: EVENT-BASED MODEL — do not change to entity-relationship without flagging first
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
