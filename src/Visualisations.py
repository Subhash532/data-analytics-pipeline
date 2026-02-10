import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px
import urllib.parse

USER = "postgres"
PASSWORD = urllib.parse.quote_plus("Postgres@2026")
HOST = "localhost"
PORT = "5432"
DB = "Analytics_WA"
engine = create_engine(f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}")

#Total vs Active users weekly (full data range)
df_users = pd.read_sql("""
select
  date_trunc('week', message_first_seen_at) as week,
  count(distinct user_id) as total_users,
  count(distinct user_id) filter (where direction = 'inbound') as active_users
from analytics.message_facts
group by 1
order by 1;
""", engine)
px.line(df_users, x="week", y=["total_users","active_users"], markers=True,
        title="Total vs Active Users (Weekly)").show()

#Fraction read (non-failed outbound)
df_frac = pd.read_sql("""
with outbound as (
  select *
  from analytics.message_facts
  where direction = 'outbound'
    and coalesce(ever_failed,false) = false
)
select
  count(*) filter (where first_read_ts is not null)::numeric
  / nullif(count(*)::numeric, 0) as fraction_read
from outbound;
""", engine)
print("Fraction of non-failed outbound messages read:", float(df_frac["fraction_read"][0]))

#Time-to-read distribution (minutes) - histogram
df_ttr = pd.read_sql("""
select
  extract(epoch from (first_read_ts - first_sent_ts))/60.0 as minutes_to_read
from analytics.message_facts
where direction = 'outbound'
  and coalesce(ever_failed,false) = false
  and first_sent_ts is not null
  and first_read_ts is not null
  and first_read_ts >= first_sent_ts;
""", engine)

df_ttr_plot = df_ttr[df_ttr["minutes_to_read"] <= df_ttr["minutes_to_read"].quantile(0.99)]

px.histogram(df_ttr_plot, x="minutes_to_read", nbins=50,
             title="Time Between Sent and Read (minutes) - p99 capped").show()

#Outbound last 7 days (relative to dataset max) by latest status
df_status = pd.read_sql("""
with max_ts as (
  select max(message_first_seen_at) as mx
  from analytics.message_facts
)
select
  coalesce(latest_status,'unknown') as latest_status,
  count(*) as message_count
from analytics.message_facts, max_ts
where direction = 'outbound'
  and message_first_seen_at >= max_ts.mx - interval '7 days'
group by 1
order by 2 desc;
""", engine)

px.bar(df_status, x="latest_status", y="message_count",
       title="Outbound Messages (Last 7 Days of Available Data) by Status").show()
