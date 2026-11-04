-- One row per public GitHub event: renamed, lightly cleaned, with common fields pulled from the payload.
-- No joins and no business logic beyond de-duplication (staging layer rule).
--
-- Semantic duplicates: some pushes appear as two PushEvents with different event_ids but the same
-- push_id, ref, head, before, actor, repo and timestamp (170 on 2026-09-27; likely at-least-once
-- delivery upstream). Silver de-duplicates on event_id only, so they survive until here.
-- Rule: for PushEvents keep one row per push_id (the smallest event_id, so reruns pick the same row).
-- Other event types are unique by event_id already.

with source as (

    select * from {{ source('gharchive', 'silver_events') }}

),

cleaned as (

    select
        event_id,
        event_type,
        created_at                                    as event_ts,
        dt                                            as event_date,
        actor_id,
        actor_login,
        repo_id,
        nullif(repo_name, '')                         as repo_name,
        org_id,
        org_login,
        json_extract_scalar(payload_json, '$.action') as action,
        payload_json
    from source
    where is_public

),

ranked as (

    select
        *,
        row_number() over (
            partition by
                case
                    when event_type = 'PushEvent'
                        then concat('push:', json_extract_scalar(payload_json, '$.push_id'))
                    else concat('event:', event_id)
                end
            order by cast(event_id as bigint)
        ) as dup_rank
    from cleaned

)

select
    event_id,
    event_type,
    event_ts,
    event_date,
    actor_id,
    actor_login,
    repo_id,
    repo_name,
    org_id,
    org_login,
    action,
    payload_json
from ranked
where dup_rank = 1
