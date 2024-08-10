from time import time
from random import randint

from delta import delta

db:delta = delta.connect(path="tutorial.delta")

data = lambda w, h: [{f"field_{n}":randint(1,10) for n in range(w)} for _ in range(h)]

# ---
start = time()
print(db.add(table="table_name", primary_key="name", data=data(100, 1_000)))
print(time()-start)
# ---

# ---
start = time()
print(db.delete("table_name", lambda row: (row["field_69"] % 2) == 0 ))
print(time()-start)
# ---

# ---
start = time()
print(db.sql("select field_43 from table_name where field_43 = 1"))
print(time()-start)
# ---

# from json import dumps
# print(dumps(data(100, 5), indent=4))

# start = time()
# print(db.add(table="table_name", primary_key="name", data=[
#     dict(name=f"name_{n}", id=n) for n in range(1_000_000)
# ]))
# print(time()-start)

# db.commit("table_name")

# start = time()
# print(db.sql("select * from table_name where id = 427635"))
# print(time()-start)

# start = time()
# print(db.delete("table_name", lambda row: (row["id"] % 2) == 0 ))
# print(time()-start)

# start = time()
# print(db.sql("select * from table_name"))
# print(time()-start)