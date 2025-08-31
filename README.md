# End-to-End YouTube Analytics Pipeline with dbt, DuckDB & Airflow

This project demonstrates a modern **data engineering workflow**:
- Ingest raw data from the **YouTube Data API** with Python
- Transform & model with **dbt** into a DuckDB warehouse
- Orchestrate with **Apache Airflow** (Dockerized)
- Run validations and generate insights


# Architecture
YouTube API → Python ingestion → Raw CSV → dbt (staging + marts in DuckDB) → Validations/Insights → Airflow orchestration

- **Ingestion**: `ingest_youtube.py` fetches video metadata (views, likes, comments, tags, publish date, duration, etc.).
- **Data Warehouse**: DuckDB (`warehouse/youtube.duckdb`).
- **Transformations**:
  - **Staging models**: clean/cast raw fields, parse durations, standardize tags
  - **Mart models**: derive KPIs like engagement ratio, publish day-of-week, content length buckets
- **Orchestration**: Airflow DAG `dbt_youtube_pipeline.py` runs daily ingestion → dbt seed → dbt run → dbt test.
- **Validation**: Python scripts (`validate_queries.py`) check row counts and top KPIs.

---

# Features

-  End-to-end data pipeline (Python + dbt + Airflow + DuckDB)
-  Real YouTube channel data via Google API
-  Analytical KPIs:
  - Engagement ratio (`(likes + comments) / views`)
  - Best publish day-of-week
  - Performance by content length (shorts, 1–2 min, long-form)
- ⚡ Fully containerized with Docker Compose
- ✅ Automated daily runs with Airflow

---

# Setup

### 1. Clone & install deps (for local dev)
```bash
git clone https://github.com/<your-username>/etl-dbt-youtube-datasource-ingestion.git
cd etl-dbt-youtube-datasource-ingestion
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

