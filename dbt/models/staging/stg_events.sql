-- One row per public GitHub event: renamed, lightly cleaned, with common fields pulled from the payload.
-- No joins and no business logic here (staging layer rule).

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

)

select * from cleaned
