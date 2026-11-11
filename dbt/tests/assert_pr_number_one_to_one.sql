-- Rule: (base_repo_id, pr_number) and pull_request_id identify the same PR, one-to-one.
-- fact_pull_request is keyed on repo + number because conversation comments carry no PR id;
-- this test checks that choice is safe in both directions.
-- Returns one row per violation (0 rows = pass).
--   direction 1: one pr_sk seen with more than one pull_request_id
--   direction 2: one pull_request_id seen under more than one pr_sk
-- Comment rows have a NULL pull_request_id and are left out.

with pairs as (

    select distinct pr_sk, pull_request_id
    from {{ ref('int_pr_events') }}
    where pull_request_id is not null

),

pr_sk_with_many_ids as (

    select
        'pr_sk has more than one pull_request_id'   as problem,
        pr_sk                                       as key_value,
        count(*)                                    as n_matches
    from pairs
    group by pr_sk
    having count(*) > 1

),

id_with_many_pr_sks as (

    select
        'pull_request_id has more than one pr_sk'   as problem,
        cast(pull_request_id as varchar)            as key_value,
        count(*)                                    as n_matches
    from pairs
    group by pull_request_id
    having count(*) > 1

)

select * from pr_sk_with_many_ids
union all
select * from id_with_many_pr_sks
