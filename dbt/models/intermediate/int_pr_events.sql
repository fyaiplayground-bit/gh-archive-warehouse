-- One row per pull-request-related event, unified across 4 event types.
-- Every row carries the PR business key (base_repo_id, pr_number) and pr_sk = md5 of it (macro pr_sk),
-- so fact_pull_request can GROUP BY pr_sk without looking anything up.
--   PullRequestEvent / PullRequestReviewEvent / PullRequestReviewCommentEvent: number in payload.pull_request
--   IssueCommentEvent on a PR (payload.issue.pull_request present): number in payload.issue; no PR id or branches

with pr_related as (

    select *
    from {{ ref('stg_events') }}
    where event_type in ('PullRequestEvent', 'PullRequestReviewEvent', 'PullRequestReviewCommentEvent')
       or (event_type = 'IssueCommentEvent'
           and json_extract(payload_json, '$.issue.pull_request') is not null)

),

unified as (

    select
        event_id,
        event_ts,
        event_date,
        event_type,
        case event_type
            when 'PullRequestEvent'              then 'pr_action'
            when 'PullRequestReviewEvent'        then 'review'
            when 'PullRequestReviewCommentEvent' then 'review_comment'
            when 'IssueCommentEvent'             then 'conversation_comment'
        end                                                                   as event_category,
        action,
        actor_id,
        actor_login,
        repo_id                                                               as base_repo_id,
        cast(coalesce(
            json_extract_scalar(payload_json, '$.pull_request.number'),
            json_extract_scalar(payload_json, '$.issue.number')
        ) as bigint)                                                          as pr_number,
        cast(json_extract_scalar(payload_json, '$.pull_request.id') as bigint)
                                                                              as pull_request_id,
        json_extract_scalar(payload_json, '$.pull_request.head.ref')          as head_ref,
        json_extract_scalar(payload_json, '$.pull_request.base.ref')          as base_ref,
        cast(json_extract_scalar(payload_json, '$.pull_request.head.repo.id') as bigint)
                                                                              as head_repo_id
    from pr_related

)

select
    {{ pr_sk('base_repo_id', 'pr_number') }} as pr_sk,
    *
from unified
