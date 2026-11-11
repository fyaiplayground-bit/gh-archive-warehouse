-- One row per GitHub account mentioned in the data. SCD Type 1: attributes are the latest values seen.
-- The login at the time of each event is kept on the fact tables instead.
--
-- Two sources, combined with union all:
--   1. event:         accounts that performed at least one event (stg_events)
--   2. snapshot_only: PR authors known only from comment snapshots (int_pr_snapshots) who performed no
--                     event in the window, e.g. a PR opened long ago that someone commented on today.
--                     Without them, fact_pull_request.author_actor_id has no match here (424 PRs /
--                     405 authors on 2026-09-27, 13 of the PRs by bots) and those PRs drop out of
--                     any "exclude bots" join.
-- Only authors missing from (1) are added (anti-join with not exists), so each account appears once and
-- the event login, which is the more recent one, wins.
-- first_seen_date / last_seen_date mean "first / last event in the data", so they are NULL for
-- snapshot_only accounts; source says why.

with event_actors as (

    select
        actor_id,
        max_by(actor_login, event_ts)       as login,
        min(event_date)                     as first_seen_date,
        max(event_date)                     as last_seen_date
    from {{ ref('stg_events') }}
    group by actor_id

),

snapshot_authors as (

    select
        pr_author_id                                  as actor_id,
        max_by(pr_author_login, last_snapshot_at)     as login
    from {{ ref('int_pr_snapshots') }}
    where pr_author_id is not null
    group by pr_author_id

),

missing_authors as (

    select s.actor_id, s.login
    from snapshot_authors s
    where not exists (
        select 1
        from event_actors e
        where e.actor_id = s.actor_id
    )

),

combined as (

    select
        actor_id,
        login,
        first_seen_date,
        last_seen_date,
        'event'                             as source
    from event_actors

    union all

    select
        actor_id,
        login,
        cast(null as date)                  as first_seen_date,
        cast(null as date)                  as last_seen_date,
        'snapshot_only'                     as source
    from missing_authors

)

select
    actor_id,
    login,
    coalesce(login like '%[bot]', false)    as is_bot,
    first_seen_date,
    last_seen_date,
    source
from combined
