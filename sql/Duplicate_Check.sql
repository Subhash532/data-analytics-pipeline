with base as (
  select
    message_uuid,
    user_id,
    direction,
    coalesce(inbound_content, outbound_rendered_content) as msg_text,
    message_first_seen_at
  from analytics.message_facts
  where message_first_seen_at is not null
),
pairs as (
  select
    a.message_uuid as a_uuid,
    b.message_uuid as b_uuid,
    a.user_id,
    a.direction,
    a.msg_text,
    abs(extract(epoch from (a.message_first_seen_at - b.message_first_seen_at))) as diff_seconds
  from base a
  join base b
    on a.user_id = b.user_id
   and a.direction = b.direction
   and a.msg_text = b.msg_text
   and a.message_uuid < b.message_uuid
)
select *
from pairs
where diff_seconds <= 60
order by diff_seconds;

