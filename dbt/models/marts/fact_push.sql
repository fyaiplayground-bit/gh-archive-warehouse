-- One row per push (transaction fact, factless: the 2026 payload has no commit count).
-- Incremental merge on push_id, re-processing the last 2 days.

{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='push_id',
    partitioned_by=['push_date']
) }}

with pushes as (

    select
        cast(json_extract_scalar(payload_json, '$.push_id') as bigint) as push_id,
        event_id,
        event_ts                                                       as pushed_at,
        actor_id,
        repo_id,
        json_extract_scalar(payload_json, '$.ref')                     as ref,
        json_extract_scalar(payload_json, '$.head')                    as head_sha,
        json_extract_scalar(payload_json, '$.before')                  as before_sha,
        event_date                                                     as push_date
    from {{ ref('stg_events') }}
    where event_type = 'PushEvent'

    {% if is_incremental() %}
      and event_date >= (select date_add('day', -2, max(push_date)) from {{ this }})
    {% endif %}

)

select
    push_id,
    event_id,
    pushed_at,
    actor_id,
    repo_id,
    ref,
    regexp_replace(ref, '^refs/heads/', '')  as branch_name,
    head_sha,
    before_sha,
    push_date
from pushes
