import pandas as pd
from sqlalchemy import create_engine,text

DB_URL = "postgresql://postgres:Postgres%402026@localhost:5432/Analytics_WA"
XLSX_PATH = r"data\Data Engineer Task Assignment.xlsx"

engine = create_engine(DB_URL)

def main():
    msgs = pd.read_excel(XLSX_PATH, sheet_name="Messages")
    stats = pd.read_excel(XLSX_PATH, sheet_name="Statuses")

    msgs.columns = [c.strip() for c in msgs.columns]
    stats.columns = [c.strip() for c in stats.columns]

    with engine.begin() as conn:
        conn.execute(text("truncate table raw.messages;"))
        conn.execute(text("truncate table raw.statuses;"))

    msgs.to_sql("messages", engine, schema="raw", if_exists="append", index=False, chunksize=5000, method="multi")
    stats.to_sql("statuses", engine, schema="raw", if_exists="append", index=False, chunksize=5000, method="multi")
 
    print(
        f"Data load completed successfully. "
        f"Inserted {len(msgs)} message rows and "
        f"{len(stats)} status rows."
    )

if __name__ == "__main__":
    main()