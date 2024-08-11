# deltadb

__deltadb__ is a lightweight, fast, simple, and massively scalable database based on [__polars__](https://github.com/pola-rs/polars) and [__deltalake__](https://github.com/delta-io/delta-rs) designed to facilitate seamless data operations, offering features like upsert, delete, commit, and version control on your datasets, while leveraging the powerful performance of polars and deltalake.

## Install
```bash
pip install deltadb
```

## Usage
```python
from deltadb import delta

# connect to a database
db = delta.connect(path="test.delta")

# upsert data into a table
db.upsert(
    table="test_table", primary_key="id", 
    data=dict(id=1, name="alice")
)

# query the data
result = db.sql("select * from test_table")
print(result)  # output: [dict(id=1, name="alice")]

# commit the changes
db.commit("test_table")
```

---

### Upsert Multiple
```python
# upsert multiple records at once, and automatically handling schema differences
db.upsert(
    table="test_table", primary_key="id", 
    data=[
        dict(id=1, name="ali"),
        dict(id=2, name="bob", job="chef"),
        dict(id=3, name="sam"),
    ]
)
```

### Query with DataFrame Response
```python
# return a polars dataframe for more advanced data operations
df_result = db.sql("select * from test_table", dtype="polars")
print(df_result)
```

### Commit with Schema Diff
```python
# use force when there is a schema difference between the current and new data
db.commit("test_table", force=True)
```

### Delete Records
```python
# delete records with sql
db.delete(table="test_table", filter="name='charles'")

# or lambda
db.delete(table="test_table", filter=lambda row: row["name"] == "charles")

# or delete a table all together
db.delete("test_table")
```

### Checkout Previous Table Version
```python
# revert tables to a previous version
db.checkout(table="test_table", version=0)
```