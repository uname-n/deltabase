You can run SQL queries against your registered tables using the `sql` method. This allows you to interact with your data using standard SQL syntax.

```python 
db.sql("select * from mytable")
```

---

> `#!python db.sql("select * from mytable", dtype="polars")`

You can specify the output format of the query using the `dtype` parameter, such as `polars` for fast data processing.

---

> `#!python db.sql("select * from mytable", lazy=True)`

If you prefer to defer execution, you can return a LazyFrame by setting the `lazy` parameter to `True`.

---
