--Total vs Active users weekly (full data range) 
select
  date_trunc('week', message_first_seen_at) as week,
  count(distinct user_id) as total_users,
  count(distinct user_id) filter (where direction = 'inbound') as active_users
from analytics.message_facts
group by 1
order by 1;

-- Fraction of non failed outbound messages that were read
with outbound as (
  select *
  from analytics.message_facts
  where direction = 'outbound'
    and coalesce(ever_failed,false) = false
)
select
  count(*) filter (where first_read_ts is not null)::numeric/ nullif(count(*)::numeric,0) as fraction_read
from outbound;

-- Distribution of time between sent and read
select
  extract(epoch from (first_read_ts - first_sent_ts))/60.0 as minutes_to_read
from analytics.message_facts
where direction = 'outbound'
  and coalesce(ever_failed,false) = false
  and first_sent_ts is not null
  and first_read_ts is not null
  and first_read_ts >= first_sent_ts;

-- Outbound of messages in the last week by status
select
  coalesce(latest_status,'unknown') as latest_status,
  count(*) as message_count
from analytics.message_facts
where direction = 'outbound'
  and message_first_seen_at >= now() - interval '7 days'
group by 1
order by 2 desc;




