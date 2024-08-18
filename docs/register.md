To load or register a table in the SQL context from a delta source, use the `register` method. You can register tables from the delta source, load a specific version, or register a DataFrame or LazyFrame directly.

---

Register a table from a specific database in the delta source:

```python 
db.register(database="mydatabase", table="mytable")
```

---

> `#!python db.register(..., alias="table")`

You can assign an alias to the registered table within the SQL context for easier reference.

---

> `#!python db.register(..., version=1)`

Load a specific version of the table by specifying the version number.

---

> `#!python db.register(..., data=...)`

Register a DataFrame or LazyFrame directly instead of loading it from the Delta source.

---

> `#!python db.register(..., pyarrow_options={"partitions": [("year", "=", "2021")]})`

Use `pyarrow_options` to specify partition filters or other advanced options when loading the table.

---
