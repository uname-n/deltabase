from time import time
from random import randint
from shutil import rmtree

from delta import delta

# rmtree("tutorial.delta")
db:delta = delta.connect(path="tutorial.delta")

data = lambda w, h: [{"name":f"name_{_}", **{f"field_{n}":n+_ for n in range(w)}} for _ in range(h)]

# testing_data = data(100, 1_000_000)
testing_data = data(10_000, 10_000)

# print("starting")
# s = time()
# # ---
# db.add(table="table_name", primary_key="name", data=testing_data)
# db.commit("table_name")
# db.delete("table_name", lambda row: (row["field_69"] % 2) == 0 )
# db.sql("select field_43 from table_name where field_43 = 1")
# # ---
# print(f"{time()-s:.2f} seconds.")

# print(db.sql("select * from table_name"))

print("starting")
s = time()
# ---
db.add(table="table_name", primary_key="name", data=testing_data)
db.commit("table_name")
db.delete("table_name", "field_6 % 2 == 0")
db.commit("table_name")

print(db.sql("select * from table_name"))
db.checkout("table_name", version=0)
print(db.sql("select * from table_name"))
# ---
print(f"{time()-s:.2f} seconds.")

# print("starting")
# s = time()
# # ---
# db.checkout("table_name", version=0)
# db.delete("table_name", "field_6 % 2 == 0")
# # db.delete("table_name", lambda row: (row["field_6"] % 2) == 0 )
# print(db.sql("select * from table_name"))
# # ---
# print(f"{time()-s:.2f} seconds.")