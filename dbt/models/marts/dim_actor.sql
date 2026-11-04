-- One row per GitHub account. SCD Type 1: attributes are the latest values seen.
-- The login at the time of each event is kept on the fact tables instead.

select
    actor_id,
    max_by(actor_login, event_ts)          as login,
    max_by(actor_login, event_ts) like '%[bot]'
                                           as is_bot,
    min(event_date)                        as first_seen_date,
    max(event_date)                        as last_seen_date
from {{ ref('stg_events') }}
group by actor_id
