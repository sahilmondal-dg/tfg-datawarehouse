-- =============================================================================
-- fct_work_order
-- Domain: D3 — Work Orders
-- Phase: 02 core (Jan-IT), 03 extended (ServiceChannel, UtilizeCore, Springshot)
-- Source(s): Jan-IT (national), ServiceChannel (subcontracted), UtilizeCore (local & regional), Springshot (aviation)
-- Description: Neutral canonical work order abstraction above all four source systems. source_wo_id preserves original system identifier for traceability.
-- Status: STUB — awaiting silver models and live data
-- Note: source_wo_id must be preserved — do not overwrite with canonical_id
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
