To delete records from a table or remove a table from the SQL context, use the `delete` method. You can delete specific records based on a condition or remove all records.

```python
# delete records with sql condition
db.delete(table="mytable", filter="name='bob'")

# delete records using a lambda function
db.delete(table="mytable", filter=lambda row: row["name"] == "sam")

# delete table from sql context
db.delete(table="mytable")
```

---
