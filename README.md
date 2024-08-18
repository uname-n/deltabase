<h1 align="center">
  <img src="./docs/assets/banner.svg" alt="banner">
  <br>
</h1>

<p align="center">
  <a href="https://uname-n.github.io/deltabase">documentation (wip)</a>
</p>

**DeltaBase** is a lightweight, comprehensive solution for managing Delta Tables in both local and cloud environments. Built on the high-performance frameworks [**polars**](https://github.com/pola-rs/polars) and [**deltalake**](https://github.com/delta-io/delta-rs), DeltaBase streamlines data operations with features like upsert, delete, commit, and version control. Designed for data engineers, analysts, and developers, it ensures data consistency, efficient versioning, and seamless integration into your workflows.

## Setup
To install **deltabase**, run the following command:
```bash
pip install deltabase
```

## Usage
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