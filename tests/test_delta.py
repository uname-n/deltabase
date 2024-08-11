import pytest

from delta import delta

from polars import DataFrame
from os.path import exists
from os import listdir
from shutil import rmtree

@pytest.fixture
def db():
    _db = delta.connect(path="test.delta")
    yield _db
    if "test_table" in _db.tables: _db.delete("test_table")
    if exists("test.delta"): rmtree("test.delta")

def test_connect(db):
    assert isinstance(db, delta)
    assert db.tables == []

def test_add_new_record(db):
    db.upsert(table="test_table", primary_key="id", data=dict(id=1, name="alice"))
    result = db.sql("select * from test_table")

    assert isinstance(result, DataFrame)
    assert result.shape == (1, 2)
    assert result["id"].to_list() == [1]
    assert result["name"].to_list() == ["alice"]

def test_add_multiple_records(db):
    db.upsert(table="test_table", primary_key="id", data=[dict(id=2, name="bob"), dict(id=3, name="charles")])
    result = db.sql("select * from test_table")

    assert result.shape == (2, 2)
    assert set(result["id"].to_list()) == {2, 3}
    assert set(result["name"].to_list()) == {"bob", "charles"}

def test_add_with_new_column(db):
    db.upsert(table="test_table", primary_key="id", data=dict(id=4, name="david", job="fisher"))
    result = db.sql("select * from test_table")

    assert result.shape == (1, 3)
    assert result["id"].to_list() == [4]
    assert result["name"].to_list() == ["david"]
    assert result["job"].to_list() == ["fisher"]

def test_delete_record(db):
    db.upsert(table="test_table", primary_key="id", data=dict(id=5, name="edward"))
    db.upsert(table="test_table", primary_key="id", data=dict(id=6, name="charles"))
    db.delete(table="test_table", filter="name='charles'")
    result = db.sql("select * from test_table")

    assert result.shape == (1, 2)
    assert "charles" not in result["name"].to_list()
    assert result["name"].to_list() == ["edward"]

def test_commit_and_versioning(db):
    db.upsert(table="test_table", primary_key="id", data=dict(id=5, name="edward"))
    db.commit("test_table")

    db.upsert(table="test_table", primary_key="id", data=dict(id=5, name="henry"))

    result = db.sql("select * from test_table")
    assert result.shape == (1, 2)
    assert result["id"].to_list() == [5]
    assert result["name"].to_list() == ["henry"]

    db.checkout(table="test_table", version=0)

    result = db.sql("select * from test_table")
    assert result.shape == (1, 2)
    assert result["id"].to_list() == [5]
    assert result["name"].to_list() == ["edward"]

def test_delete_table(db):
    db.upsert(table="test_table", primary_key="id", data=dict(id=5, name="george"))
    db.commit("test_table")

    result = db.sql("select * from test_table")
    
    assert result.shape == (1, 2)
    assert result["id"].to_list() == [5]
    assert result["name"].to_list() == ["george"]

    assert exists("test.delta/test_table")
    assert len(listdir("test.delta/test_table")) == 2
    assert len(listdir("test.delta/test_table/_delta_log")) == 1

    db.delete("test_table")

    assert exists("test.delta/test_table")
    assert "test_table" not in db.tables

    db.commit("test_table")

    assert not exists("test.delta/test_table")