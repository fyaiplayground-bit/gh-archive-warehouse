-- One row per pull request that had at least one conversation comment in the data.
-- IssueCommentEvent payloads carry a full snapshot of the PR's underlying issue record
-- (PullRequestEvent payloads are trimmed), so comments tell us when the PR was created and by whom,
-- even if the PR was opened before our data window. Verified on 2026-09-27: 62/62 PRs with both an
-- 'opened' event and a comment had the same author and creation time in the snapshot.

with comments as (

    select
        repo_id                                                                       as base_repo_id,
        cast(json_extract_scalar(payload_json, '$.issue.number') as bigint)           as pr_number,
        event_ts,
        cast(from_iso8601_timestamp(json_extract_scalar(payload_json, '$.issue.created_at')) as timestamp)
                                                                                      as pr_created_at,
        cast(json_extract_scalar(payload_json, '$.issue.user.id') as bigint)          as pr_author_id,
        json_extract_scalar(payload_json, '$.issue.user.login')                       as pr_author_login,
        cast(from_iso8601_timestamp(json_extract_scalar(payload_json, '$.issue.pull_request.merged_at')) as timestamp)
                                                                                      as merged_at,
        cast(from_iso8601_timestamp(json_extract_scalar(payload_json, '$.issue.closed_at')) as timestamp)
                                                                                      as closed_at,
        json_extract_scalar(payload_json, '$.issue.state')                            as state
    from {{ ref('stg_events') }}
    where event_type = 'IssueCommentEvent'
      and json_extract(payload_json, '$.issue.pull_request') is not null

)

select
    {{ pr_sk('base_repo_id', 'pr_number') }}  as pr_sk,
    base_repo_id,
    pr_number,
    min(pr_created_at)                        as pr_created_at,
    max_by(pr_author_id, event_ts)            as pr_author_id,
    max_by(pr_author_login, event_ts)         as pr_author_login,
    max(merged_at)                            as snapshot_merged_at,
    max(closed_at)                            as snapshot_closed_at,
    max_by(state, event_ts)                   as snapshot_state,
    max(event_ts)                             as last_snapshot_at
from comments
group by base_repo_id, pr_number
