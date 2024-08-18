To insert new records or update existing records in a table, use the `upsert` method. This method allows you to keep your data synchronized while managing schema changes automatically.

---

Insert or update a single record:

```python
db.upsert(database="mydatabase", table="mytable", primary_key="id", data={
    "id": 1, 
    "name": "alice"
})
```

---

Insert or update multiple records at once:

```python
db.upsert(database="mydatabase", table="mytable", primary_key="id", data=[
    {"id": 1, "name": "ali"},
    {"id": 2, "name": "bob", "job": "chef"},
    {"id": 3, "name": "sam"},
])
```

---

> `#!python db.upsert(..., data=DataFrame([{"id": 1, "name": "ali"}]))`

You can upsert data directly from a DataFrame.

---

> `#!python db.upsert(..., data=LazyFrame([{"id": 1, "name": "ali"}]))`

Or, upsert data using a LazyFrame for more efficient operations.

---
