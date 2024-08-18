<h1 align="center">
  <img src="./docs/assets/banner.svg" alt="banner">
  <br>
</h1>

<p align="center">
  <a href="https://uname-n.github.io/deltadb">documentation</a>
</p>

**deltabase** is a lightweight comprehensive solution for managing Delta Tables in both local and cloud environments. Built on [**polars**](https://github.com/pola-rs/polars) and [**deltalake**](https://github.com/delta-io/delta-rs), it is designed to streamline data operations, providing features like upsert, delete, commit, and version control while harnessing the high performance of **polars** and **deltalake**. Whether you're a data engineer, analyst, or developer, DeltaBase empowers you to efficiently handle complex data operations, ensuring data consistency, versioning, and seamless integration with your workflows.

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