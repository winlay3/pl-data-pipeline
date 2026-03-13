from elasticsearch import Elasticsearch
import pandas as pd

# Connect to Elasticsearch
es = Elasticsearch('http://localhost:9200')
print("Connected:", es.ping())

# Delete old index if exists
if es.indices.exists(index="pl_standings"):
    es.indices.delete(index="pl_standings")
    print("Old index deleted!")

# Create fresh index
es.indices.create(index="pl_standings")
print("Index created!")

# Read CSV
df = pd.read_csv(r'C:\Users\User\Desktop\simple_PL\data\pl_table.csv')

# Insert each row
for i, r in df.iterrows():
    doc = {
        "idx": r['idx'],
        "name": r['name'],
        "played": r['played'],
        "wins": r['wins'],
        "draws": r['draws'],
        "losses": r['losses'],
        "scoresStr": r['scoresStr'],
        "goalConDiff": r['goalConDiff'],
        "pts": r['pts']
    }
    es.index(index="pl_standings", document=doc)
    print(f"Inserted: {r['name']}")

print("All data inserted! ✅")