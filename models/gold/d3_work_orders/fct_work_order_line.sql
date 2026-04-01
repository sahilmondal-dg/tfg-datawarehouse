-- =============================================================================
-- fct_work_order_line
-- Domain: D3 — Work Orders
-- Phase: 02 core (Jan-IT), 03 extended (ServiceChannel, UtilizeCore, Springshot)
-- Source(s): Jan-IT, ServiceChannel, UtilizeCore, Springshot
-- Description: Line-level detail for canonical work orders. Captures per-service quantities and costs.
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
