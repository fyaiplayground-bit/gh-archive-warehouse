-- Rule: for PRs where we see both an 'opened' event and a comment snapshot, the snapshot's author and
-- creation time agree with the event. fact_pull_request relies on this when it fills opened_at and the
-- author from the snapshot for PRs opened before the window (verified 62/62 on 2026-09-27).
-- This tests an assumption about the source data, not our own code, so it warns instead of failing
-- the build: a few exceptions in a year of data should be looked at, not block every model downstream.
-- Returns one row per disagreeing PR (0 rows = pass).

{{ config(severity = 'warn') }}

with opened_events as (

    select
        pr_sk,
        min(event_ts)                    as opened_event_at,
        min_by(actor_id, event_ts)       as opened_event_actor_id
    from {{ ref('int_pr_events') }}
    where event_category = 'pr_action'
      and action = 'opened'
    group by pr_sk

)

select
    o.pr_sk,
    o.opened_event_at,
    s.pr_created_at,
    date_diff('second', s.pr_created_at, o.opened_event_at)   as seconds_apart,
    o.opened_event_actor_id,
    s.pr_author_id
from opened_events o
join {{ ref('int_pr_snapshots') }} s
    on o.pr_sk = s.pr_sk
where o.opened_event_actor_id is distinct from s.pr_author_id
   or abs(date_diff('second', s.pr_created_at, o.opened_event_at)) > 5
