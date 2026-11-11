-- Rule: every event with a known repo_id falls into exactly one dim_repo version
-- (point-in-time join: valid_from <= event_ts < valid_to).
--   0 versions -> a gap in the intervals: the fact row would silently go to the unknown member '-1'
--   2+ versions -> overlapping intervals: duplicate fact keys, the merge fails with MULTIPLE_MATCHES
-- Events with a NULL repo_id are excluded: by design they link to the unknown member.
-- Returns one row per bad event (0 rows = pass).

with events as (

    select event_id, repo_id, event_ts
    from {{ ref('stg_events') }}
    where repo_id is not null

),

matches as (

    select
        e.event_id,
        e.repo_id,
        e.event_ts,
        count(d.repo_sk) as n_versions      -- count(column) skips NULLs, so no match = 0
    from events e
    left join {{ ref('dim_repo') }} d
        on  e.repo_id  = d.repo_id
        and e.event_ts >= d.valid_from
        and e.event_ts <  d.valid_to
    group by e.event_id, e.repo_id, e.event_ts

)

select *
from matches
where n_versions <> 1
