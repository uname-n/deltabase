# deltadb

**deltadb** is a lightweight, fast, and scalable database built on [**polars**](https://github.com/pola-rs/polars) and [**deltalake**](https://github.com/delta-io/delta-rs). It is designed to streamline data operations, providing features like upsert, delete, commit, and version control while harnessing the high performance of polars and deltalake.

## Installation
To install __deltadb__, run the following command:

```bash
pip install deltadb
```

## Key Features

- **Upsert**: Efficiently insert or update records in your tables.
- **Delete**: Remove specific records or entire tables.
- **Commit**: Version control your data with commit functionality.
- **Query**: Execute SQL queries and retrieve results as dictionaries or dataframes.
- **Schema Handling**: Automatically manage schema changes during data operations.
- **Versioning**: Revert tables to previous versions when needed.

## Getting Started

### Connecting to a Database
Establish a connection to your database:

```python
from deltadb import delta

# Connect to a database at the specified path
db = delta.connect(path="test.delta")
```

### Upserting Data
Insert or update data within a table:

```python
db.upsert(
    table="test_table", 
    primary_key="id", 
    data={"id": 1, "name": "alice"}
)

# Query the data
result = db.sql("SELECT * FROM test_table")
print(result)  # Output: [{'id': 1, 'name': 'alice'}]

# Commit the changes
db.commit("test_table")
```

### Upserting Multiple Records
Upsert multiple records simultaneously, with automatic schema management:

```python
db.upsert(
    table="test_table", 
    primary_key="id", 
    data=[
        {"id": 1, "name": "ali"},
        {"id": 2, "name": "bob", "job": "chef"},
        {"id": 3, "name": "sam"},
    ]
)
```

### Querying Data as a DataFrame
Execute SQL queries and return the results as a polars DataFrame for advanced data manipulation:

```python
df_result = db.sql("SELECT * FROM test_table", dtype="polars")
print(df_result)
```

### Committing with Schema Differences
Force a commit even when there are schema differences between the current and new data:

```python
db.commit("test_table", force=True)
```

### Deleting Records
Remove specific records from a table using SQL or lambda functions, or delete the entire table:

```python
# Delete records using an SQL filter
db.delete(table="test_table", filter="name='charles'")

# Delete records using a lambda function
db.delete(table="test_table", filter=lambda row: row["name"] == "charles")

# Delete the entire table
db.delete("test_table")
```

### Checking Out Previous Table Versions
Revert to a previous version of a table with ease:

```python
db.checkout(table="test_table", version=0)
```