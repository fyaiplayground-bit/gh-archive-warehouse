-- One row per GitHub event type, with the business process it belongs to.
-- Source of truth is the seed file seeds/event_types.csv (version-controlled).

select
    event_type,
    business_process,
    description
from {{ ref('event_types') }}
