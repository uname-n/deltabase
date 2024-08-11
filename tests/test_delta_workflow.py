import pytest

from delta import delta

from polars import DataFrame
from os.path import exists
from os import listdir
from shutil import rmtree
from time import time

W = 10_000
H = 10_000

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
    db.add(table="test_table", primary_key="name", data=testing_data)
    e = time()
    result = db.sql("select * from test_table")

    assert isinstance(result, DataFrame)
    assert result.shape == (W, H+1)
    assert (e-s) < 20

def test_first_commit(db):
    db.commit("test_table")
    assert exists("test.delta/test_table")
    assert len(listdir("test.delta/test_table")) == 2
    assert len(listdir("test.delta/test_table/_delta_log")) == 1

def test_delete_rows(db):
    db.delete("test_table", "field_6 % 2 == 0")
    result = db.sql("select * from test_table")
    assert 0 < result.shape[0] < W
    assert result.shape[1] == H+1

def test_second_commit(db):
    db.commit("test_table")
    assert len(listdir("test.delta/test_table")) == 3
    assert len(listdir("test.delta/test_table/_delta_log")) == 2

def test_checkout_original_commit(db):
    db.checkout("test_table", version=0)
    result = db.sql("select * from test_table")

    assert isinstance(result, DataFrame)
    assert result.shape == (W, H+1)