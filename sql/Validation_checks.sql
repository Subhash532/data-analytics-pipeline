-- Validation Checks which included like Outbound Messages missing statutses,Read without sent and Negative time to read

select count(*) as outbound_missing_statuses
from analytics.message_facts
where direction = 'outbound'
  and jsonb_array_length(statuses_history) = 0;


select count(*) as read_without_sent
from analytics.message_facts
where direction = 'outbound'
  and coalesce(ever_failed,false) = false
  and first_read_ts is not null
  and first_sent_ts is null;


select count(*) as negative_time_to_read
from analytics.message_facts
where direction = 'outbound'
  and first_sent_ts is not null
  and first_read_ts is not null
  and first_read_ts < first_sent_ts;
