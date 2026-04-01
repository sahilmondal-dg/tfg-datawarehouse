-- =============================================================================
-- dim_service_location
-- Domain: D1 — Customers & Locations
-- Phase: 02 (Core Systems, Wks 4–15)
-- Source(s): NetSuite (master), Jan-IT, ServiceChannel, UtilizeCore, Springshot
-- Description: Canonical service location dimension. Resolves same physical site across multiple source systems.
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
