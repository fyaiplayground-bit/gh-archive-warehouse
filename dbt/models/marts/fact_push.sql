-- One row per push (transaction fact, factless: the 2026 payload has no commit count).
-- Incremental merge on push_id, re-processing the last 2 days.
-- repo_sk: point-in-time join to dim_repo at pushed_at; no match -> '-1' (unknown member).

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
    p.push_id,
    p.event_id,
    p.pushed_at,
    p.actor_id,
    p.repo_id,
    coalesce(d.repo_sk, '-1')                    as repo_sk,
    p.ref,
    regexp_replace(p.ref, '^refs/heads/', '')    as branch_name,
    p.head_sha,
    p.before_sha,
    p.push_date
from pushes p
left join {{ ref('dim_repo') }} d
    on  p.repo_id   = d.repo_id
    and p.pushed_at >= d.valid_from
    and p.pushed_at <  d.valid_to
