# file: validate_queries.py
import duckdb
import pandas as pd
from pathlib import Path

DB_PATH = Path("warehouse/youtube.duckdb")

def q(con, sql):
    df = con.execute(sql).fetchdf()
    print(sql.strip(), "\n")
    print(df, "\n" + "-"*80 + "\n")
    return df

def main():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Database not found at {DB_PATH}. "
                                "Run `dbt run --profiles-dir profiles` first.")
    con = duckdb.connect(str(DB_PATH))

    # 1) Top videos by engagement
    q(con, """
        select title, view_count, like_count, comment_count, engagement_ratio
        from youtube.mart_video_performance
        order by engagement_ratio desc nulls last
        limit 10
    """)

    # 2) Performance by length bucket
    q(con, """
        select length_bucket, count(*) as n, avg(engagement_ratio) as avg_eng
        from youtube.mart_video_performance
        group by 1
        order by n desc
    """)

    # 3) Best publish weekday (by avg engagement)
    q(con, """
        select publish_dow, avg(engagement_ratio) as avg_eng
        from youtube.mart_video_performance
        group by 1
        order by avg_eng desc nulls last
    """)

if __name__ == "__main__":
    main()
