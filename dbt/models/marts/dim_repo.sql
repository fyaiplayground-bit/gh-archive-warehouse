-- One row per repository version. SCD Type 2 on organization ownership (org_id), Type 1 on the name.
-- Built from event history using event time (not dbt snapshot, which would stamp processing time and
-- break on backfills). Gaps and islands: flag org changes with lag(), running-sum the flags into a
-- version number, group by version.
-- org_id null = personal repo. Checked on 2026-09-27: 93 of 801,937 repos changed ownership, each
-- exactly once (no flip-flopping), so nulls are real "personal" states, not missing data.
-- The exact transfer moment is unknown; a new version starts at its first event (approximation).
-- Intervals are half-open [valid_from, valid_to): first version starts 1900-01-01, current ends 9999-12-31.
-- Includes an unknown-member row (repo_sk = '-1') so fact foreign keys are never null.

with events as (

    select repo_id, event_id, event_ts, repo_name, org_id, org_login
    from {{ ref('stg_events') }}

),

flagged as (

    select
        *,
        case
            when row_number() over (partition by repo_id order by event_ts, event_id) = 1 then 1
            when org_id is distinct from
                 lag(org_id) over (partition by repo_id order by event_ts, event_id)  then 1
            else 0
        end as is_change
    from events

),

versioned as (

    select
        *,
        sum(is_change) over (partition by repo_id order by event_ts, event_id
                             rows between unbounded preceding and current row) as version_num
    from flagged

),

versions as (

    select
        repo_id,
        version_num,
        min(event_ts)                    as first_event_ts,
        max_by(org_id, event_ts)         as org_id,
        max_by(org_login, event_ts)      as org_login
    from versioned
    group by repo_id, version_num

),

bounded as (

    select
        *,
        case when version_num = 1 then timestamp '1900-01-01 00:00:00'
             else first_event_ts end                                         as valid_from,
        coalesce(lead(first_event_ts) over (partition by repo_id order by version_num),
                 timestamp '9999-12-31 00:00:00')                            as valid_to
    from versions

),

latest_name as (

    select
        repo_id,
        max_by(repo_name, case when repo_name is not null then event_ts end) as repo_name
    from events
    group by repo_id

)

select
    {{ surrogate_key(['b.repo_id', 'b.valid_from']) }} as repo_sk,
    b.repo_id,
    n.repo_name,
    split_part(n.repo_name, '/', 1)                  as owner_login,
    split_part(n.repo_name, '/', 2)                  as short_name,
    b.org_id,
    b.org_login,
    b.valid_from,
    b.valid_to,
    b.valid_to = timestamp '9999-12-31 00:00:00'     as is_current,
    b.version_num
from bounded b
join latest_name n
    on b.repo_id = n.repo_id

union all

select
    '-1'                                             as repo_sk,
    cast(-1 as bigint)                               as repo_id,
    'Unknown'                                        as repo_name,
    cast(null as varchar)                            as owner_login,
    cast(null as varchar)                            as short_name,
    cast(null as bigint)                             as org_id,
    cast(null as varchar)                            as org_login,
    timestamp '1900-01-01 00:00:00'                  as valid_from,
    timestamp '9999-12-31 00:00:00'                  as valid_to,
    true                                             as is_current,
    0                                                as version_num
