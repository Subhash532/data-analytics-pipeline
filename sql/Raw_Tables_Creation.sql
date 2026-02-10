drop table if exists raw.messages;
create table raw.messages (
  content text,
  id bigint,
  message_type text,
  masked_addressees text,
  masked_author text,
  author_type text,
  direction text,
  external_id text,
  external_timestamp timestamp,
  masked_from_addr text,
  is_deleted boolean,
  last_status text,
  last_status_timestamp timestamp,
  rendered_content text,
  source_type text,
  uuid uuid,
  inserted_at timestamp,
  updated_at timestamp
);

drop table if exists raw.statuses;
create table raw.statuses (
  id bigint,
  status text,
  timestamp timestamp,
  uuid uuid,
  message_uuid uuid,
  message_id bigint,
  number_id bigint,
  inserted_at timestamp,
  updated_at timestamp
);

create index if not exists idx_messages_uuid on raw.messages(uuid);
create index if not exists idx_statuses_message_uuid on raw.statuses(message_uuid);
create index if not exists idx_messages_inserted_at on raw.messages(inserted_at);
create index if not exists idx_statuses_timestamp on raw.statuses(timestamp);
