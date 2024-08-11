# delta

__delta__ is a lightweight, fast, simple, and massively scalable database based on [__polars__](https://github.com/pola-rs/polars) and [__deltalake__](https://github.com/delta-io/delta-rs) designed to facilitate seamless data operations, offering features like upsert, delete, commit, and version control on your datasets, while leveraging the powerful performance of polars and deltalake.

### pip
```bash
pip install deltadb
```

### usage
```python
from deltadb import delta

# connect to a delta table
db = delta.connect(path="test.delta")

# Upsert data into the table
db.upsert(
    table="test_table", primary_key="id", 
    data=dict(id=1, name="alice")
)

# Query the data
result = db.sql("select * from test_table")
print(result)  # Output: [dict(id=1, name="alice")]

# Commit the changes
db.commit("test_table")
```