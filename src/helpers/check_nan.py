from src.helpers.database import Firebase
import pandas as pd
db = Firebase()

tickets = db.read('db/clients/contacts', {})

df = pd.DataFrame(tickets)

# Give me all rows that have a nan somewhere
nan_rows = df[df.isna().any(axis=1)]
print(nan_rows)

for index, row in nan_rows.iterrows():
    print(row)
