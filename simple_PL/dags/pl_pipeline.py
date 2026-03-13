import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch




# ================================
# Task 1 - Load CSV to PostgreSQL
# ================================
def loadToPostgresql():
    conn_string = "dbname='DataEngineering' host='host.docker.internal' user='postgres' password='yourpassword' port='5432'"
    conn = db.connect(conn_string)
    cur = conn.cursor()

    # Read CSV
    df = pd.read_csv('/opt/airflow/dags/pl_table.csv')

    # Clear old data
    cur.execute("DELETE FROM pl_standings")

    # Insert data
    for i, r in df.iterrows():
        cur.execute("""
            INSERT INTO pl_standings
            (idx, name, played, wins, draws, losses, scoresStr, goalConDiff, pts)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            r['idx'],
            r['name'],
            r['played'],
            r['wins'],
            r['draws'],
            r['losses'],
            r['scoresStr'],
            r['goalConDiff'],
            r['pts']
        ))

    conn.commit()
    conn.close()
    print("-------Data Loaded to PostgreSQL------")

# ================================
# Task 2 - Load PostgreSQL to Elasticsearch
# ================================
def loadToElasticsearch():
    conn_string = "dbname='DataEngineering' host='host.docker.internal' user='postgres' password='yourpassword' port='5432'"
    conn = db.connect(conn_string)
    df = pd.read_sql("select * from pl_standings", conn)
    conn.close()

    es = Elasticsearch('http://host.docker.internal:9200')

    if es.indices.exists(index="pl_standings"):
        es.indices.delete(index="pl_standings")

    es.indices.create(index="pl_standings")

    for i, r in df.iterrows():
        doc = {
            "idx": int(r['idx']),
            "name": r['name'],
            "played": int(r['played']),
            "wins": int(r['wins']),
            "draws": int(r['draws']),
            "losses": int(r['losses']),
            "scoresstr": r['scoresstr'],      # lowercase!
            "goalcondiff": int(r['goalcondiff']),  # lowercase!
            "pts": int(r['pts'])
        }
        es.index(index="pl_standings", document=doc)
        print(f"Inserted: {r['name']}")

    print("-------Data Loaded to Elasticsearch------")

# ================================
# Task 3 - Load LaLiga to PostgreSQL
# ================================
def loadLaLigaToPostgresql():
    conn_string = "dbname='DataEngineering' host='host.docker.internal' user='postgres' password='yourpassword' port='5432'"
    conn = db.connect(conn_string)
    cur = conn.cursor()

    df = pd.read_csv('/opt/airflow/dags/standings.csv')
    cur.execute("DELETE FROM laliga_standings")

    for i, r in df.iterrows():
        cur.execute("""
            INSERT INTO laliga_standings
            (position, club, played, wins, draws, losses, gf, ga, gd, points)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            r['Position'],
            r['Club'],
            r['Played'],
            r['Wins'],
            r['Draws'],
            r['Losses'],
            r['GF'],
            r['GA'],
            r['GD'],
            r['Points']
        ))

    conn.commit()
    conn.close()
    print("-------LaLiga Data Loaded to PostgreSQL------")

# ================================
# Task 4 - Load LaLiga to Elasticsearch
# ================================
def loadLaLigaToElasticsearch():
    conn_string = "dbname='DataEngineering' host='host.docker.internal' user='postgres' password='yourpassword' port='5432'"
    conn = db.connect(conn_string)
    df = pd.read_sql("select * from laliga_standings", conn)
    conn.close()

    es = Elasticsearch('http://host.docker.internal:9200')

    if es.indices.exists(index="laliga_standings"):
        es.indices.delete(index="laliga_standings")
        print("Old index deleted!")

    es.indices.create(index="laliga_standings")

    for i, r in df.iterrows():
        doc = {
            "position": int(r['position']),
            "club": r['club'],
            "played": int(r['played']),
            "wins": int(r['wins']),
            "draws": int(r['draws']),
            "losses": int(r['losses']),
            "gf": int(r['gf']),
            "ga": int(r['ga']),
            "gd": int(r['gd']),
            "points": int(r['points'])
        }
        es.index(index="laliga_standings", document=doc)
        print(f"Inserted: {r['club']}")

    print("-------LaLiga Data Loaded to Elasticsearch------")

# ================================
# DAG arguments
# ================================
default_args = {
    'owner': 'winmyintkyaw',
    'start_date': dt.datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5)
}

# ================================
# DAG definition
# ================================
with DAG('FootballPipeline',
         default_args=default_args,
         schedule=timedelta(days=1),
) as dag:

    loadPL = PythonOperator(
        task_id='LoadPLToPostgreSQL',
        python_callable=loadToPostgresql
    )

    loadPLElastic = PythonOperator(
        task_id='LoadPLToElasticsearch',
        python_callable=loadToElasticsearch
    )

    loadLaLiga = PythonOperator(
        task_id='LoadLaLigaToPostgreSQL',
        python_callable=loadLaLigaToPostgresql
    )

    loadLaLigaElastic = PythonOperator(
        task_id='LoadLaLigaToElasticsearch',
        python_callable=loadLaLigaToElasticsearch
    )

    # PL pipeline
    loadPL >> loadPLElastic

    # LaLiga pipeline runs parallel!
    loadLaLiga >> loadLaLigaElastic