<a name="top"></a>
# DeltaDB

**DeltaDB** is a lightweight, fast, and scalable database built on [**polars**](https://github.com/pola-rs/polars) and [**deltalake**](https://github.com/delta-io/delta-rs). It is designed to streamline data operations, providing features like upsert, delete, commit, and version control while harnessing the high performance of polars and deltalake.

## Table of Contents

- [Installation](#installation)
- [Key Features](#key-features)
- [Getting Started](#getting-started)
    - [Connecting to a Database](#connecting-to-a-database)
    - [Upserting Data](#upserting-data)
    - [Upserting Multiple Records](#upserting-multiple-records)
    - [Querying Data as a DataFrame](#querying-data-as-a-dataframe)
    - [Committing with Schema Differences](#committing-with-schema-differences)
    - [Deleting Records](#deleting-records)
    - [Checking Out Previous Table Versions](#checking-out-previous-table-versions)
- [Performance](#performance)

## Installation
To install __DeltaDB__, run the following command:

```bash
pip install deltadb
```
<sub>[[return]](#top)</sub>

## Key Features

- **Upsert**: Efficiently insert or update records in your tables.
- **Delete**: Remove specific records or entire tables.
- **Commit**: Version control your data with commit functionality.
- **Query**: Execute SQL queries and retrieve results as dictionaries or dataframes.
- **Schema Handling**: Automatically manage schema changes during data operations.
- **Versioning**: Revert tables to previous versions when needed.

<sub>[[return]](#top)</sub>

## Getting Started

### Connecting to a Database 
Establish a connection to your database:

```python
from deltadb import delta

# connect to a database at the specified path
db = delta.connect(path="test.delta")
```
<sub>[[return]](#top)</sub>

### Upserting Data 
Insert or update data within a table:

```python
db.upsert(
    table="test_table", 
    primary_key="id", 
    data={"id": 1, "name": "alice"}
)

# query the data
result = db.sql("select * from test_table")
print(result)  # output: [{'id': 1, 'name': 'alice'}]

# commit the changes
db.commit("test_table")
```
<sub>[[return]](#top)</sub>

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
<sub>[[return]](#top)</sub>

### Querying Data as a DataFrame 
Execute SQL queries and return the results as a polars DataFrame for advanced data manipulation:

```python
df_result = db.sql("select * from test_table", dtype="polars")
print(df_result)
```
<sub>[[return]](#top)</sub>

### Committing with Schema Differences 
Force a commit even when there are schema differences between the current and new data:

```python
db.commit("test_table", force=True)
```
<sub>[[return]](#top)</sub>

### Deleting Records 
Remove specific records from a table using SQL or lambda functions, or delete the entire table:

```python
# delete records using an sql filter
db.delete(table="test_table", filter="name='charles'")

# delete records using a lambda function
db.delete(table="test_table", filter=lambda row: row["name"] == "charles")

# delete the entire table
db.delete("test_table")
```
<sub>[[return]](#top)</sub>

### Checking Out Previous Table Versions 
Revert to a previous version of a table with ease:

```python
db.checkout(table="test_table", version=0)
```
<sub>[[return]](#top)</sub>

## Performance

**DeltaDB** excels in performance, particularly when compared to traditional databases like SQLite. A series of benchmarks were conducted to compare the average elapsed time for operations in both **DeltaDB** and SQLite.

### Benchmark Setup

The benchmarks involved running 100 iterations of data insertion into tables of varying sizes, specifically with a width of 1,000 and a height of 10,000, to simulate realistic data load scenarios. For each iteration, the time taken to perform the operations was measured, and the average elapsed time for both **DeltaDB** and SQLite was calculated.

### Results

- **DeltaDB** demonstrated a significant performance advantage, especially as the data size increased. For a table with a width of 1,000 and a height of 10,000, **DeltaDB** completed the operations in an average time of **1.03 seconds** over 100 runs.
- In contrast, SQLite took an average of **8.06 seconds** for the same operations and data size, making **DeltaDB** approximately **87.22% faster**.

These results underscore the efficiency of **DeltaDB** in handling large datasets and performing complex operations, positioning it as a strong choice for data-intensive applications.

<sub>[[return]](#top)</sub>
