drop table if exists analytics.message_facts;

create table analytics.message_facts as
with message_versions as (
  select
    m.uuid as message_uuid,
	
    case
      when m.direction = 'inbound' then m.masked_author
      when m.direction = 'outbound' then m.masked_addressees
      else null
    end as user_id,

    jsonb_agg(
      jsonb_build_object(
        'id', m.id,
        'message_type', m.message_type,
        'masked_addressees', m.masked_addressees,
        'masked_author', m.masked_author,
        'author_type', m.author_type,
        'direction', m.direction,
        'external_id', m.external_id,
        'external_timestamp', m.external_timestamp,
        'masked_from_addr', m.masked_from_addr,
        'is_deleted', m.is_deleted,
        'last_status', m.last_status,
        'last_status_timestamp', m.last_status_timestamp,
        'content', m.content,
        'rendered_content', m.rendered_content,
        'source_type', m.source_type,
        'inserted_at', m.inserted_at,
        'updated_at', m.updated_at
      )
      order by m.inserted_at, m.updated_at
    ) as message_history,

    min(m.inserted_at) as message_first_seen_at,
    max(m.updated_at)  as message_last_updated_at,

    (array_agg(m.direction order by m.updated_at desc nulls last, m.inserted_at desc nulls last))[1] as direction,
    (array_agg(m.message_type order by m.updated_at desc nulls last, m.inserted_at desc nulls last))[1] as message_type,
    (array_agg(m.author_type order by m.updated_at desc nulls last, m.inserted_at desc nulls last))[1] as author_type,
    (array_agg(m.content order by m.updated_at desc nulls last, m.inserted_at desc nulls last))[1] as inbound_content,
    (array_agg(m.rendered_content order by m.updated_at desc nulls last, m.inserted_at desc nulls last))[1] as outbound_rendered_content
  from raw.messages m
  group by
    m.uuid,
    case
      when m.direction = 'inbound' then m.masked_author
      when m.direction = 'outbound' then m.masked_addressees
      else null
    end
),
status_history as (
  select
    s.message_uuid,

    jsonb_agg(
      jsonb_build_object(
        'status_id', s.id,
        'status', s.status,
        'status_timestamp', s.timestamp,
        'number_id', s.number_id,
        'inserted_at', s.inserted_at,
        'updated_at', s.updated_at
      )
      order by s.timestamp, s.inserted_at
    ) as statuses_history,

    (array_agg(s.status order by s.timestamp desc nulls last, s.inserted_at desc nulls last))[1] as latest_status,
    (array_agg(s.timestamp order by s.timestamp desc nulls last, s.inserted_at desc nulls last))[1] as latest_status_timestamp,

    min(case when s.status = 'sent' then s.timestamp end) as first_sent_ts,
    min(case when s.status = 'read' then s.timestamp end) as first_read_ts,
    bool_or(s.status = 'failed') as ever_failed
  from raw.statuses s
  group by s.message_uuid
)
select
  mv.message_uuid,
  mv.user_id,
  mv.direction,
  mv.message_type,
  mv.author_type,
  mv.inbound_content,
  mv.outbound_rendered_content,
  mv.message_first_seen_at,
  mv.message_last_updated_at,
  sh.latest_status,
  sh.latest_status_timestamp,
  sh.first_sent_ts,
  sh.first_read_ts,
  sh.ever_failed,
  mv.message_history,
  coalesce(sh.statuses_history, '[]'::jsonb) as statuses_history
from message_versions mv
left join status_history sh
  on mv.message_uuid = sh.message_uuid;
