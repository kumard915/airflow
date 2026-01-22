import requests
import pandas as pd
from datetime import datetime, timezone
from sqlalchemy import create_engine, text
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ---------------------------
# HTTP SESSION WITH RETRY
# ---------------------------
def get_session():
    retry = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


# ---------------------------
# DB ENGINE
# ---------------------------
engine = create_engine(
    "postgresql+psycopg2://airflow:airflow@postgres:5432/analytics"
)


# ---------------------------
# SOURCES
# ---------------------------
SOURCES = {
    "payin": "http://host.docker.internal:4000/payins",
    "payout": "http://host.docker.internal:4000/payouts",
    "merchant": "http://host.docker.internal:4000/merchants",
    "account": "http://host.docker.internal:4000/accounts"
}

TIMESTAMP_COL = {
    "payin": "created_on",
    "payout": "created_on",
    "merchant": "created_on",
    "account": "created_on"
}

DEFAULT_TS = datetime(1970, 1, 1, tzinfo=timezone.utc)


# ---------------------------
# WATERMARK FUNCTIONS
# ---------------------------
def get_watermark(source):
    with engine.begin() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS etl_watermark (
                source_name TEXT PRIMARY KEY,
                last_processed_ts TIMESTAMPTZ
            )
        """)

        result = c.execute(
            text("""
                SELECT last_processed_ts
                FROM etl_watermark
                WHERE source_name = :s
            """),
            {"s": source}
        ).scalar()

        if result is None:
            c.execute(
                text("""
                    INSERT INTO etl_watermark (source_name, last_processed_ts)
                    VALUES (:s, :ts)
                """),
                {"s": source, "ts": DEFAULT_TS}
            )
            return DEFAULT_TS

        return result


def update_watermark(source, ts):
    with engine.begin() as c:
        c.execute(
            text("""
                UPDATE etl_watermark
                SET last_processed_ts = :t
                WHERE source_name = :s
            """),
            {"s": source, "t": ts}
        )


# ---------------------------
# BRONZE ETL
# ---------------------------
def run_bronze():
    session = get_session()

    for source, url in SOURCES.items():
        last_ts = get_watermark(source)

        page = 1
        max_ts_seen = last_ts
        total_inserted = 0

        while True:
            r = session.get(url, params={"page": page}, timeout=(5, 120))
            r.raise_for_status()

            payload = r.json()
            records = payload.get("data", [])
            meta = payload.get("meta", {})

            if not records:
                break

            df = pd.json_normalize(records)

            ts_col = TIMESTAMP_COL[source]
            if ts_col not in df.columns:
                raise ValueError(f"{source} missing column {ts_col}")

            # ✅ UTC-safe timestamps
            df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce", utc=True)

            # ✅ APPLY WATERMARK FILTER (CRITICAL)
            df = df[df[ts_col] > last_ts]

            if df.empty:
                if not meta.get("hasNextPage"):
                    break
                page += 1
                continue

            page_max_ts = df[ts_col].max()
            max_ts_seen = max(max_ts_seen, page_max_ts)

            df.to_sql(
                f"bronze_{source}s",
                engine,
                if_exists="append",
                index=False
            )

            total_inserted += len(df)

            if not meta.get("hasNextPage"):
                break

            page += 1

        # ✅ Update watermark ONLY if new rows inserted
        if total_inserted > 0:
            update_watermark(source, max_ts_seen)

        print(
            f"✅ {source}: inserted={total_inserted}, watermark={max_ts_seen}"
        )

# working code 21 at 16 55 pm error bronz load full data every time 
# import requests
# import pandas as pd
# from datetime import datetime,timezone
# from sqlalchemy import create_engine, text
# # 
# from requests.adapters import HTTPAdapter
# from urllib3.util.retry import Retry

# def get_session():
#     retry = Retry(
#         total=3,
#         backoff_factor=2,
#         status_forcelist=[500, 502, 503, 504],
#         allowed_methods=["GET"]
#     )

#     adapter = HTTPAdapter(max_retries=retry)
#     session = requests.Session()
#     session.mount("http://", adapter)
#     session.mount("https://", adapter)
#     return session


# engine = create_engine(
#     "postgresql+psycopg2://airflow:airflow@postgres:5432/analytics"
# )

# # SOURCES = {
# #     "payin": "http://host.docker.internal:4000/generate/payin",
# #     "payout": "http://host.docker.internal:4000/generate/payout",
# #     "merchant": "http://host.docker.internal:4000/generate/merchant",
# #     "account": "http://host.docker.internal:4000/generate/account"
# # }
# SOURCES = {
#     "payin": "http://host.docker.internal:4000/payins",
#     "payout": "http://host.docker.internal:4000/payouts",
#     "merchant": "http://host.docker.internal:4000/merchants",
#     "account": "http://host.docker.internal:4000/accounts"
# }


# DEFAULT_TS = datetime(1970, 1, 1, tzinfo=timezone.utc)

# def get_watermark(source):
#     with engine.begin() as c:
#         # ✅ Ensure table exists
#         c.execute("""
#             CREATE TABLE IF NOT EXISTS etl_watermark (
#                 source_name TEXT PRIMARY KEY,
#                 last_processed_ts TIMESTAMP
#             )
#         """)

#         q = text("""
#             SELECT last_processed_ts
#             FROM etl_watermark
#             WHERE source_name = :s
#         """)

#         result = c.execute(q, {"s": source}).scalar()

#         if result is None:
#             # ✅ Insert default watermark
#             c.execute(
#                 text("""
#                     INSERT INTO etl_watermark (source_name, last_processed_ts)
#                     VALUES (:s, :ts)
#                 """),
#                 {"s": source, "ts": DEFAULT_TS}
#             )
#             return DEFAULT_TS

#         return result  # always datetime


# def update_watermark(source, ts):
#     with engine.begin() as c:
#         c.execute(
#             text("""
#                 UPDATE etl_watermark
#                 SET last_processed_ts = :t
#                 WHERE source_name = :s
#             """),
#             {"t": ts, "s": source}
#         )
# # TIMESTAMP_COL = {
# #     "payin": "createdOn",
# #     "payout": "createdOn",
# #     "merchant": "createdOn",
# #     "account": "createdOn"
# # }
# TIMESTAMP_COL = {
#     "payin": "created_on",
#     "payout": "created_on",
#     "merchant": "created_on",
#     "account": "created_on"
# }
# def run_bronze():
#     session = get_session()

#     for source, url in SOURCES.items():
#         last_ts = get_watermark(source)

#         page = 1
#         max_ts_seen = last_ts
#         total_inserted = 0

#         while True:
#             try:
#                 r = session.get(
#                     url,
#                     params={"page": page},
#                     timeout=(5, 120)
#                 )
#                 r.raise_for_status()
#             except Exception as e:
#                 raise RuntimeError(f"API failed for {source} page {page}: {e}")

#             payload = r.json()
#             records = payload.get("data", [])
#             meta = payload.get("meta", {})

#             if not records:
#                 break

#             df = pd.json_normalize(records)

#             ts_col = TIMESTAMP_COL[source]
#             if ts_col not in df.columns:
#                 raise ValueError(
#                     f"{source} missing expected timestamp column {ts_col}. "
#                     f"Available columns: {list(df.columns)}"
#                 )

#             df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce")
#             page_max_ts = df[ts_col].max()

#             if pd.notna(page_max_ts):
#                 max_ts_seen = max(max_ts_seen, page_max_ts)

#             df.to_sql(
#                 f"bronze_{source}s",
#                 engine,
#                 if_exists="append",
#                 index=False
#             )

#             total_inserted += len(df)

#             if not meta.get("hasNextPage"):
#                 break

#             page += 1

#         if total_inserted > 0:
#             update_watermark(source, max_ts_seen)

#         print(
#             f"✅ Bronze loaded {total_inserted} rows for {source}, "
#             f"last_ts={max_ts_seen}"
#         )


#  21 -01-2026
# def run_bronze():
#     session = get_session()

#     for source, url in SOURCES.items():
#         last_ts = get_watermark(source)

#         try:
#             r = session.get(
#                 url,
#                 params={"count": 3000},
#                 timeout=(5, 60)
#             )
#             r.raise_for_status()
#         except Exception as e:
#             raise RuntimeError(f"API failed for {source}: {e}")

#         payload = r.json()
#         records = payload.get("data", [])

#         if not records:
#             continue

#         df = pd.json_normalize(records)

#         ts_col = TIMESTAMP_COL[source]

#         if ts_col not in df.columns:
#             raise ValueError(
#                 f"{source} missing expected timestamp column {ts_col}. "
#                 f"Available columns: {list(df.columns)}"
#             )

#         df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce")

#         max_ts = df[ts_col].max()
#         if pd.isna(max_ts):
#             raise ValueError(f"{source} has no valid timestamps in {ts_col}")

#         df.to_sql(
#             f"bronze_{source}s",
#             engine,
#             if_exists="append",
#             index=False
#         )

#         update_watermark(source, max_ts)


# def run_bronze():
#     session = get_session()

#     for source, url in SOURCES.items():
#         last_ts = get_watermark(source)

#         try:
#             r = session.get(
#                 url,
#                 params={"from_ts": last_ts.isoformat()},
#                 timeout=(5, 60)
#             )
#             r.raise_for_status()
#         except Exception as e:
#             raise RuntimeError(f"API failed for {source}: {e}")

#         payload = r.json()
#         records = payload.get("data", [])

#         if not records:
#             continue

#         df = pd.json_normalize(records)

#         ts_col = TIMESTAMP_COL[source]

#         if ts_col not in df.columns:
#           raise ValueError(
#         f"{source} missing expected timestamp column {ts_col}. "
#         f"Available columns: {list(df.columns)}"
#          )
#         df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce")
#         max_ts = df[ts_col].max()
#         if pd.isna(max_ts):
#          raise ValueError(f"{source} has no valid timestamps in {ts_col}")
#         else:
#             max_ts = last_ts

#         df.to_sql(
#             f"bronze_{source}s",
#             engine,
#             if_exists="append",
#             index=False
#         )

#         update_watermark(source, max_ts)


# def run_bronze():
#     for source, url in SOURCES.items():
#         last_ts = get_watermark(source)

#         try:
#             r = requests.get(
#                 url,
#                 params={"from_ts": last_ts.isoformat()},
#                 timeout=10
#             )
#             r.raise_for_status()
#             data = r.json()
#         except Exception as e:
#             raise RuntimeError(f"API failed for {source}: {e}")

#         if not data:
#             continue

#         df = pd.DataFrame(data)

#         df["createdOn"] = pd.to_datetime(df["createdOn"])

#         df.to_sql(
#             f"bronze_{source}s",
#             engine,
#             if_exists="append",
#             index=False
#         )

#         update_watermark(source, df["createdOn"].max())


# import requests
# import pandas as pd
# from sqlalchemy import create_engine, text

# engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/analytics")

# SOURCES = {
#     "payin": "http://host.docker.internal:4000/generate/payin",
#     "payout": "http://host.docker.internal:4000/generate/payout",
#     "merchant": "http://host.docker.internal:4000/generate/merchant",
#     "account": "http://host.docker.internal:4000/generate/account"
# }

# def get_watermark(source):
#     q = text("SELECT last_processed_ts FROM etl_watermark WHERE source_name=:s")
#     with engine.begin() as c:
#         return c.execute(q, {"s": source}).scalar()

# def update_watermark(source, ts):
#     q = text("UPDATE etl_watermark SET last_processed_ts=:t WHERE source_name=:s")
#     with engine.begin() as c:
#         c.execute(q, {"t": ts, "s": source})

# def run_bronze():
#     for source, url in SOURCES.items():
#         last_ts = get_watermark(source)
#         r = requests.get(url, params={"from_ts": last_ts.isoformat()})
#         data = r.json()
#         if not data:
#             continue
#         df = pd.DataFrame(data)
#         df.to_sql(f"bronze_{source}s", engine, if_exists="append", index=False)
#         update_watermark(source, pd.to_datetime(df["createdOn"]).max())
