import pytest

from deltabase import delta

from polars import DataFrame
from os.path import exists
from os import listdir
from time import time

TIMEOUT = 20
W = 1_000
H = 1_000

data = lambda w, h: [{"name":f"name_{_}", **{f"field_{n}":n+_ for n in range(w)}} for _ in range(h)]
testing_data = data(W, H)

@pytest.fixture
def db():
    return delta.connect(path="test.delta")

def test_connect(db):
    assert isinstance(db, delta)
    assert db.tables == []

def test_add_data(db):
    s = time()
    err = db.upsert(table="test_table", primary_key="name", data=testing_data)
    assert not err, err
    e = time()
    result = db.sql("select * from test_table", dtype="polars")

    assert isinstance(result, DataFrame)
    assert result.shape == (W, H+1)
    assert (e-s) < TIMEOUT

def test_first_commit(db):
    err = db.commit("test_table")
    assert not err, err

    assert exists("test.delta/default/test_table")
    assert len(listdir("test.delta/default/test_table")) == 2
    assert len(listdir("test.delta/default/test_table/_delta_log")) == 1

def test_delete_records(db):
    err = db.delete("test_table", filter="field_6 % 2 == 0")
    assert not err, err

    result = db.sql("select * from test_table", dtype="polars")
    assert 0 < result.shape[0] < W
    assert result.shape[1] == H+1

def test_second_commit(db):
    err = db.commit("test_table")
    assert not err, err

    assert len(listdir("test.delta/default/test_table")) == 3
    assert len(listdir("test.delta/default/test_table/_delta_log")) == 2

def test_checkout_original_commit(db):
    err = db.checkout("test_table", version=1)
    assert not err, err

    result = db.sql("select * from test_table", dtype="polars")

    assert isinstance(result, DataFrame)
    assert result.shape == (W, H+1)