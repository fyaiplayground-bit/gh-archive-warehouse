-- One row per public event (transaction fact, factless: the measure is COUNT(*)).
-- Incremental merge on event_id, re-processing the last 2 days to catch late-arriving data.
-- repo_sk (FK to dim_repo) is added in Step 5.6, followed by a --full-refresh.

{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id',
    partitioned_by=['event_date']
) }}

select
    event_id,
    event_ts,
    event_type,
    actor_id,
    repo_id,
    action,
    actor_login                as actor_login_at_event,
    repo_name                  as repo_name_at_event,
    event_date
from {{ ref('stg_events') }}

{% if is_incremental() %}
where event_date >= (select date_add('day', -2, max(event_date)) from {{ this }})
{% endif %}
