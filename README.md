# Data Analytics Pipeline

## Overview
This project implements an end-to-end data analytics pipeline using WhatsApp-style messaging data.The raw data consists of append-only message and status tables.
The pipeline:
        ingests raw data into PostgreSQL
        transforms it into a single analytical table with one row per message while preserving full history
        performs data quality and consistency checks
        generates key engagement metrics and visualizations

## Tech Stack
- PostgreSQL (local)
- Python (pandas, SQLAlchemy, Plotly)
- pgAdmin
- Git / GitHub

## Data Ingestion
- Raw data is provided as an Excel file with two sheets:Messages and Statuses
- Data is loaded into PostgreSQL tables: raw.messages and raw.statuses
- Tables are append-only and the load process is re-runnable.

Row counts after ingestion:
- Messages: 14,747
- Statuses: 32,950


## Data Transformation
- Created an analytical table: `analytics.message_facts`
- Derived analytical fields for convenience:user_id,latest_status,first_sent_ts,first_read_ts,ever_failed

Final transformed row count: analytics.message_facts: 14,746


## Data Validation & Quality Checks
The following checks were performed:
Duplicate detection - Identical message content from the same user within a short time window
Outbound messages missing statuses- Count: 497
Read without sent- Count: 1925
Negative time-to-read-  Count: 2


## Visualizations
All visualizations are generated only from `analytics.message_facts`

1. Total vs Active Users Over Time
    - Active users are those who sent inbound messages and full available data range used

2. Fraction of Non-failed Outbound Messages Read
   - 84.3% of non-failed outbound messages were read

3. Time Between Sent and Read : 
   - Highly skewed distribution
   - Many messages read within minutes
   - Long tail extending to days/weeks

4. Outbound Messages by Status (Last 7 Days of Available Data)
   - Majority reached read status
   - Smaller proportions in delivered, sent, and failed

---

# How to Run the Pipeline

- Create PostgreSQL Database by using this command : CREATE DATABASE Analytics_WA;
- Run the SQL Script - Raw Tables creation and run the Script Data Ingestion
- Later run the Scripts Transformation,Duplicatae check and Validation Checks
- Finally run the Visualisations Script where it generate all required plots and metrics using `analytics.message_facts`
