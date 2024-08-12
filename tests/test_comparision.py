import pytest

from deltadb import delta
from sqlite3 import connect
from os.path import exists
from shutil import rmtree

data = lambda w, h: [{"name":f"name_{_}", **{f"field_{n}":str(n) for n in range(w)}} for _ in range(h)]

@pytest.fixture
def db():
    _db = delta.connect(path="test.delta")
    yield _db
    if "test_table" in _db.tables: _db.delete("test_table")
    if exists("test.delta"): rmtree("test.delta")

@pytest.fixture
def testing_data(width, height):
    return data(width, height)

@pytest.fixture
def conn():
    return connect('test.db')

def test_delta(db, testing_data, width, height):
    db.upsert(table="test_table", primary_key="name", data=testing_data)
    db.commit("test_table")

    result = db.sql("select * from test_table", dtype="polars")

    assert result.shape == (height, width+1)

def test_sql(conn, testing_data, height, width):
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