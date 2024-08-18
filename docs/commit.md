To persist changes made in the SQL context back to the delta source, use the `commit` method. You can enforce schema changes or partition your data during the commit process.

```python 
db.commit(database="mydatabase", table="mytable")
```

---

> `#!python db.commit(..., force=True)`

Force schema changes when committing data to the delta source.

---

> `#!python db.commit(..., partition_by=["job"])`

Partition the table by one or more columns when committing data.

---
