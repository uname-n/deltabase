<h4 align="center">
  <img src="./docs/assets/banner.svg" alt="banner">
</h4>

<p align="center">
  <a href="https://uname-n.github.io/deltabase">documentation (wip)</a>
</p>

**DeltaBase** is a lightweight, comprehensive solution for managing Delta Tables in both local and cloud environments. Built on the high-performance frameworks [**polars**](https://github.com/pola-rs/polars) and [**deltalake**](https://github.com/delta-io/delta-rs), DeltaBase streamlines data operations with features like upsert, delete, commit, and version control. Designed for data engineers, analysts, and developers, it ensures data consistency, efficient versioning, and seamless integration into your workflows.

## Installation
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

See a full example of **DeltaBase** in action [here](https://github.com/uname-n/deltabase/blob/master/examples/magic.ipynb).

## Usage

### Connecting to a Delta Source
Establish a connection to your Delta source, whether it's a local directory or remote cloud storage.

```python
from deltabase import delta

db = delta.connect(path="local_path/mydelta")
db = delta.connect(path="s3://your-bucket/path")
db = delta.connect(path="az://your-container/path")
db = delta.connect(path="abfs[s]://your-container/path")
```

### Register Tables
Load tables into the SQL context from the Delta source using the `register` method. You can also register data directly from a DataFrame or specify options like version and alias.

```python
# load existing table from delta
db.register(table="mytable")

# load under an alias
db.register(table="mytable", alias="table_alias")

# load a specific version
db.register(table="mytable", version=1)

# load data directly
data = DataFrame([{"id": 1, "name": "Alice"}])
db.register(table="mytable", data=data)

# load with pyarrow options
db.register(
    table="mytable",
    pyarrow_options={"partitions": [("year", "=", "2021")]}
)
```

### Running SQL Queries
Execute SQL queries against your registered tables using the `sql` method.

```python
# run a query and get the result in json format
result = db.sql("select * from mytable")

# get the result as a polars dataframe
result = db.sql("select * from mytable", dtype="polars")

# return a LazyFrame for deferred execution
result = db.sql("select * from mytable", lazy=True)
```

### Upserting Data
Insert new records or update existing ones using the `upsert` method. It automatically handles schema changes and efficiently synchronizes data.

```python
# upsert a single record
db.upsert(
    table="mytable",
    primary_key="id",
    data={"id": 1, "name": "Alice"}
)

# upsert multiple records
db.upsert(
    table="mytable",
    primary_key="id",
    data=[
        {"id": 2, "name": "Bob", "job": "Chef"},
        {"id": 3, "name": "Sam"},
    ]
)

# upsert dataframes
data = DataFrame([{"id": 4, "name": "Dave"}])
db.upsert(table="mytable", primary_key="id", data=data)

# upsert lazyframes
data = LazyFrame([{"id": 5, "name": "Eve"}])
db.upsert(table="mytable", primary_key="id", data=data)
```

### Committing Changes
Persist changes made in the SQL context back to the Delta source using the `commit` method. You can enforce schema changes or partition your data during this process.


```python
db.commit(table="mytable")
db.commit(table="mytable", force=True)
db.commit(table="mytable", partition_by=["job"])
```

### Deleting Data
Remove records from a table or delete the table from the SQL context using the delete method.

```python
# delete records using a sql condition
db.delete(table="mytable", filter="name='Bob'")

# delete records using a lambda function
db.delete(table="mytable", filter=lambda row: row["name"] == "Sam")

# delete table from sql context
db.delete(table="mytable")
```

### Checking Out Previous Versions
Revert to a previous version of a table using the `checkout` method. This is useful for loading historical data or restoring a previous state.

```python
# get a specific version by number
db.checkout(table="mytable", version=1)

# get out a version by date string
db.checkout(table="mytable", version="2024-01-01")

# get out a version by datetime object
db.checkout(table="mytable", version=datetime(2024, 1, 1))
```

### Configuring Output Data Types
Set the output data format by adjusting the `dtype` attribute in the configuration object. The default format is `json`.

```python
# set output data type to polars dataframe
db.config.dtype = "polars"

# run a sql query and get results as polars dataframe
result = db.sql("SELECT * FROM mytable")
```

### Jupyter Notebook Magic
**DeltaBase** provides magic commands for use in Jupyter notebooks, enhancing your interactive data exploration experience. Magic commands are automatically enabled when you connect to delta source within a notebook.

#### Using SQL Magic
```sql
%%sql
select * from mytable
```

#### Using AI Magic
```sql
%%ai
what data is available to me?
```