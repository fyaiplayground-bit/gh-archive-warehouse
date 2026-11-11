-- One row per public event (transaction fact, factless: the measure is COUNT(*)).
-- Incremental merge on event_id, re-processing the last 2 days to catch late-arriving data.
-- repo_sk: point-in-time join to dim_repo (the repo version valid when the event happened);
-- no match (e.g. an event with a null repo_id) -> '-1', the unknown member, so no row drops out of joins.
-- The 2-day lookback also re-links recent events if dim_repo gains a new version after they were loaded.

{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id',
    partitioned_by=['event_date']
) }}

with events as (

    select *
    from {{ ref('stg_events') }}

    {% if is_incremental() %}
    where event_date >= (select date_add('day', -2, max(event_date)) from {{ this }})
    {% endif %}

)

select
    e.event_id,
    e.event_ts,
    e.event_type,
    e.actor_id,
    e.repo_id,
    coalesce(d.repo_sk, '-1')    as repo_sk,
    e.action,
    e.actor_login                as actor_login_at_event,
    e.repo_name                  as repo_name_at_event,
    e.event_date
from events e
left join {{ ref('dim_repo') }} d
    on  e.repo_id  = d.repo_id
    and e.event_ts >= d.valid_from
    and e.event_ts <  d.valid_to
