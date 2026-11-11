-- One row per pull request (accumulating snapshot). Key: pr_sk = md5(base_repo_id, pr_number).
-- Built from int_pr_events (all PR-related events) + int_pr_snapshots (PR snapshot carried by comments).
-- Milestones come from events when we saw them, otherwise from the comment snapshot; opened_at_source
-- records which. Incremental: find PRs touched in the last 2 days and recompute them from ALL their events.
-- base_repo_sk: point-in-time join to dim_repo as of the PR's opening (opened_at, else first_event_at):
-- the organization the PR was raised under. No match -> '-1' (unknown member).

{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='pr_sk'
) }}

with events as (

    select *
    from {{ ref('int_pr_events') }}

    {% if is_incremental() %}
    where pr_sk in (
        select pr_sk
        from {{ ref('int_pr_events') }}
        where event_date >= (select date_add('day', -2, max(cast(last_event_at as date))) from {{ this }})
    )
    {% endif %}

),

per_pr as (

    select
        pr_sk,
        base_repo_id,
        pr_number,
        max(pull_request_id)                                                                  as pull_request_id,

        -- latest non-null value (base_ref can change during a PR's life; comments carry no branches)
        max_by(head_ref,     case when head_ref     is not null then event_ts end)           as head_ref,
        max_by(base_ref,     case when base_ref     is not null then event_ts end)           as base_ref,
        max_by(head_repo_id, case when head_repo_id is not null then event_ts end)           as head_repo_id,

        -- milestones seen as events (conditional aggregation)
        min(case when event_category = 'pr_action' and action = 'opened'   then event_ts end) as opened_event_at,
        min_by(actor_id,
               case when event_category = 'pr_action' and action = 'opened' then event_ts end) as opened_event_actor_id,
        min(case when event_category = 'pr_action' and action = 'merged'   then event_ts end) as merged_event_at,
        max(case when event_category = 'pr_action' and action = 'closed'   then event_ts end) as closed_event_at,
        max(case when event_category = 'pr_action' and action = 'reopened' then event_ts end) as last_reopened_at,

        -- counts (additive)
        count_if(event_category = 'review')                                                   as review_count,
        count_if(event_category = 'review_comment')                                           as review_comment_count,
        count_if(event_category = 'conversation_comment')                                     as conversation_comment_count,
        count_if(event_category = 'pr_action' and action = 'labeled')                         as label_added_count,
        count_if(event_category = 'pr_action' and action = 'unlabeled')                       as label_removed_count,
        count_if(event_category = 'pr_action' and action = 'assigned')                        as assign_count,
        count_if(event_category = 'pr_action' and action = 'reopened')                        as reopen_count,

        -- distinct count (non-additive across PRs)
        count(distinct actor_id)                                                              as participant_count,

        min(event_ts)                                                                         as first_event_at,
        max(event_ts)                                                                         as last_event_at
    from events
    group by pr_sk, base_repo_id, pr_number

),

joined as (

    select
        p.*,
        s.pr_created_at,
        s.pr_author_id,
        s.snapshot_merged_at,
        s.snapshot_closed_at
    from per_pr p
    left join {{ ref('int_pr_snapshots') }} s
        on p.pr_sk = s.pr_sk

),

milestones as (

    select
        *,
        coalesce(opened_event_at, pr_created_at)            as opened_at,
        case
            when opened_event_at is not null then 'event'
            when pr_created_at   is not null then 'snapshot'
        end                                                 as opened_at_source,
        coalesce(opened_event_actor_id, pr_author_id)       as author_actor_id,
        coalesce(merged_event_at, snapshot_merged_at)       as merged_at,
        coalesce(closed_event_at, snapshot_closed_at)       as closed_at
    from joined

),

final as (

    select
        *,
        case
            when merged_at is not null then 'merged'
            when closed_at is not null
                 and (last_reopened_at is null or last_reopened_at < closed_at) then 'closed_unmerged'
            else 'open'
        end                                                 as final_state,
        coalesce(opened_at, first_event_at)                 as repo_as_of_ts
    from milestones

)

select
    f.pr_sk,
    f.base_repo_id,
    coalesce(d.repo_sk, '-1')                               as base_repo_sk,
    f.pr_number,
    f.pull_request_id,
    f.head_repo_id,
    f.head_ref,
    f.base_ref,
    f.head_repo_id <> f.base_repo_id                        as is_from_fork,
    f.author_actor_id,

    f.opened_at,
    f.opened_at_source,
    f.merged_at,
    f.closed_at,
    cast(f.opened_at as date)                               as opened_date,
    cast(f.merged_at as date)                               as merged_date,
    cast(f.closed_at as date)                               as closed_date,
    f.final_state,

    case when f.opened_at is not null and f.merged_at is not null
         then cast(date_diff('second', f.opened_at, f.merged_at) as double) / 3600 end
                                                            as hours_to_merge,
    case when f.final_state = 'closed_unmerged' and f.opened_at is not null
         then cast(date_diff('second', f.opened_at, f.closed_at) as double) / 3600 end
                                                            as hours_to_close_unmerged,
    f.opened_at is not null
        and (f.merged_at is not null or f.closed_at is not null) as is_full_lifecycle,

    f.review_count,
    f.review_comment_count,
    f.conversation_comment_count,
    f.label_added_count,
    f.label_removed_count,
    f.assign_count,
    f.reopen_count,
    f.participant_count,

    f.first_event_at,
    f.last_event_at
from final f
left join {{ ref('dim_repo') }} d
    on  f.base_repo_id  = d.repo_id
    and f.repo_as_of_ts >= d.valid_from
    and f.repo_as_of_ts <  d.valid_to
