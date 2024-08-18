# Getting Started

## Install
To install **DeltaBase**, run the following command:
```bash
pip install deltabase
```

## Quick Start
```python
from deltabase import delta

# connect to a delta source
db:delta = delta.connect(path="mydelta")

# upsert records into a table 
db.upsert(table="mytable", primary_key="id", data=[
    {"id": 1, "name": "alice"}
])

# commit table to delta source
db.commit(table="mytable")

# read records from sql context
result = db.sql("select * from mytable")
print(result) # output: [{"id": 1, "name": "alice"}]
```