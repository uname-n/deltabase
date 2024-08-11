# delta

__delta__ is a lightweight, fast, simple, and massively scalable database based on [__polars__](https://github.com/pola-rs/polars) and [__deltalake__](https://github.com/delta-io/delta-rs) designed to facilitate seamless data operations, offering features like upsert, delete, commit, and version control on your datasets, while leveraging the powerful performance of polars and deltalake.


### Key Features
- __Easy Connection__: Connect to your delta tables with a simple connect method.
- __Data Manipulation__: Perform upserts, deletes, and commits effortlessly.
- __Version Control__: Checkout previous versions of your tables to revert or inspect historical data.
- __Flexible Querying__: Execute SQL queries with options to receive results in different formats, such as Polars DataFrames or JSON.
- __Schema Management__: Automatically handle schema differences during data commits.

## Installation
To install __delta__, simply run:

```bash
pip install delta
```

## Usage
### Basic Usage
Connect to a delta table and perform basic operations like upserting data, querying, and committing changes.

```python
from delta import delta

# Connect to a delta table
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

### Upsert Multiple Records with Schema Mismatch
Upsert multiple records at once, even if their schemas don't match exactly. The system will automatically handle the schema differences.

```python
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
Retrieve the query results as a Polars DataFrame for more advanced data manipulation.

```python
df_result = db.sql("select * from test_table", dtype="polars")
print(df_result)
```

### Commit with Schema Diff
Force a commit even when there is a schema difference between the current and new data.

```python
db.commit("test_table", force=True)
```

### Delete
#### Delete Records with SQL
Delete records from a table using a SQL filter.
```python
db.delete(table="test_table", filter="name='charles'")
```

#### Delete Records with Lambda
Alternatively, delete records using a lambda function as the filter.
```python
db.delete(table="test_table", filter=lambda row: row["name"] == "charles")
```

#### Delete Records with Lambda
To delete an entire table from the delta source:
```python
db.delete("test_table")
```

### Checkout Previous Table Version
Revert to a previous version of your table, useful for rollback operations or inspecting historical data.
```python
db.checkout(table="test_table", version=0)
```