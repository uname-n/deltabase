from deltadb import delta
from sqlite3 import connect
from os.path import exists
from shutil import rmtree
from os import remove
from time import time

data = lambda w, h: [{"name":f"name_{_}", **{f"field_{n}":str(n) for n in range(w)}} for _ in range(h)]

def elapsed(func):
    def wrapper(*args, **kwargs):
        s = time()
        func(*args, **kwargs)
        return time()-s
    return wrapper

# = = = 

def setup_delta():
    db = delta.connect(path="test.delta")
    if "test_table" in db.tables: db.delete("test_table")
    if exists("test.delta"): rmtree("test.delta")
    return db

@elapsed
def test_delta(db, testing_data, width, height):
    db.upsert(table="test_table", primary_key="name", data=testing_data)
    db.commit("test_table")
    result = db.sql("select * from test_table", dtype="polars")
    assert result.shape == (height, width+1)

# = = = 

def setup_sql():
    if exists("test.db"): remove("test.db")
    conn = connect("test.db")
    return conn

@elapsed
def test_sql(conn, testing_data, width, height):
    cursor = conn.cursor()

    first_row = testing_data[0]
    columns = ", ".join([f"{key} TEXT" for key in first_row.keys()])

    create_table_query = f"CREATE TABLE IF NOT EXISTS test_table ({columns})"
    cursor.execute(create_table_query)

    placeholders = ", ".join([f":{key}" for key in first_row.keys()])
    insert_query = f"INSERT INTO test_table ({', '.join(first_row.keys())}) VALUES ({placeholders})"

    for item in testing_data:
        cursor.execute(insert_query, item)
    conn.commit()

    result = cursor.execute("select * from test_table").fetchall()
    assert len(result) == height
    assert len(result[0]) == width+1

    conn.close()

# = = = 

# n = 100
# width = 1_000
# height = 10_000
# testing_data = data(width, height)

# delta_runs = []
# for _ in range(n):
#     print(f"delta: {str(_).zfill(3)} of {str(n).zfill(3)}")
#     db = setup_delta()
#     e = test_delta(db, testing_data, width, height)
#     delta_runs.append(e)

# delta_avg = sum(delta_runs)/len(delta_runs)
# print(f"delta over {str(n).zfill(3)} runs had an average elapsed time of {delta_avg:.2f}")
# # delta over 100 runs had an average elapsed time of 0.13 1k-1k
# # delta over 100 runs had an average elapsed time of 1.03 1k-10k

# sql_runs = []
# for _ in range(n):
#     print(f"sql: {str(_).zfill(3)} of {str(n).zfill(3)}")
#     conn = setup_sql()
#     e = test_sql(conn, testing_data, width, height)
#     sql_runs.append(e)

# sql_avg = sum(sql_runs)/len(sql_runs)
# print(f"  sql over {str(n).zfill(3)} runs had an average elapsed time of {sql_avg:.2f}")
# # sql over 100 runs had an average elapsed time of 0.82 1k-1k
# # sql over 100 runs had an average elapsed time of 8.06 1k-10k

print(f"{(8.06 - 1.03) / 8.06 * 100:.2f} pct")
# 87.22