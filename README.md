# Football Data Pipeline 🏆

## Overview
Data pipeline for Premier League and La Liga 
2023/24 season standings

## Tech Stack
- Python
- PostgreSQL
- Elasticsearch
- Kibana
- Apache Airflow
- Docker

## Data Source
- Premier League 2023/24 standings
- La Liga 2023/24 standings

## Pipeline Flow
CSV → PostgreSQL → Elasticsearch → Kibana

## How to Run
1. Install requirements:
pip install psycopg2-binary elasticsearch pandas

2. Set up PostgreSQL and create tables

3. Run scripts manually:
python scripts/load_to_postgres.py
python scripts/load_to_elastic.py

4. Or run via Airflow:
Copy dags/pl_pipeline.py to airflow dags folder
Trigger FootballPipeline DAG

## Dashboard
Kibana visualizations showing:
- Points table
- Wins/Draws/Losses
