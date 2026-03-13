import pandas as pd
df = pd.read_csv(r'C:\Users\User\Desktop\simple_PL\data\standings.csv')
print(df.columns.tolist())
print(df.head())