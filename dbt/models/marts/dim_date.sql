-- One row per calendar day, 2011-01-01 to 2026-12-31 (GH Archive starts in Feb 2011).
-- Generated, not loaded: sequence() builds the list of days, unnest() turns it into rows.

with days as (

    select d as date_day
    from unnest(sequence(date '2011-01-01', date '2026-12-31', interval '1' day)) as t(d)

)

select
    date_day,
    year(date_day)                                   as year,
    quarter(date_day)                                as quarter,
    month(date_day)                                  as month,
    date_format(cast(date_day as timestamp), '%M')   as month_name,
    cast(date_trunc('week', date_day) as date)       as week_start_date,
    day_of_week(date_day)                            as day_of_week,
    date_format(cast(date_day as timestamp), '%W')   as day_name,
    day_of_week(date_day) in (6, 7)                  as is_weekend
from days
