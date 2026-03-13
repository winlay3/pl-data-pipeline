import pandas as pd
import psycopg2 as db
from dotenv import load_dotenv
import os
# Read CSV
df = pd.read_csv(r'C:\Users\User\Desktop\simple_PL\data\pl_table.csv')
print("CSV loaded!")
print(df.head())

# Connect to PostgreSQL
conn_string = f"dbname='{os.getenv('PG_DB')}' host='{os.getenv('PG_HOST')}' user='{os.getenv('PG_USER')}' password='{os.getenv('PG_PASSWORD')}' port='5432'"
conn = db.connect(conn_string)
cur = conn.cursor()

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
print("Data inserted to PostgreSQL! ✅")
conn.close()