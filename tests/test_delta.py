import pytest

from deltadb import delta

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
    result = db.sql("select * from test_table", dtype="polars")

    assert isinstance(result, DataFrame)
    assert result.shape == (1, 2)
    assert result["id"].to_list() == [1]
    assert result["name"].to_list() == ["alice"]

def test_add_multiple_records(db):
    db.upsert(table="test_table", primary_key="id", data=[dict(id=2, name="bob"), dict(id=3, name="charles")])
    result = db.sql("select * from test_table", dtype="polars")

    assert result.shape == (2, 2)
    assert set(result["id"].to_list()) == {2, 3}
    assert set(result["name"].to_list()) == {"bob", "charles"}

def test_add_mismatch_schema_records(db):
    db.upsert(table="test_table", primary_key="id", data=[dict(id=2, name="bob", job="chef"), dict(id=3, name="charles")])
    result = db.sql("select * from test_table", dtype="polars")

    assert result.shape == (2, 3)
    assert set(result["id"].to_list()) == {2, 3}
    assert set(result["name"].to_list()) == {"bob", "charles"}

def test_json_output(db):
    db.upsert(table="test_table", primary_key="id", data=dict(id=5, name="james"))
    result = db.sql("select * from test_table", dtype="json")
    assert isinstance(result, list)
    assert isinstance(result[0], dict)
    assert result[0] == dict(id=5, name="james")
    
def test_delete_record_sql(db):
    db.upsert(table="test_table", primary_key="id", data=dict(id=5, name="edward"))
    db.upsert(table="test_table", primary_key="id", data=dict(id=6, name="charles"))
    db.delete(table="test_table", filter="name='charles'")
    result = db.sql("select * from test_table", dtype="polars")

    assert result.shape == (1, 2)
    assert "charles" not in result["name"].to_list()
    assert result["name"].to_list() == ["edward"]

def test_delete_record_lambda(db):
    db.upsert(table="test_table", primary_key="id", data=dict(id=5, name="edward"))
    db.upsert(table="test_table", primary_key="id", data=dict(id=6, name="charles"))
    db.delete(table="test_table", filter=lambda row: row["name"] == "charles")
    result = db.sql("select * from test_table", dtype="polars")

    assert result.shape == (1, 2)
    assert "charles" not in result["name"].to_list()
    assert result["name"].to_list() == ["edward"]

def test_schema_override(db):
    db.upsert(table="test_table", primary_key="id", data=dict(id=5, name="edward"))
    db.commit("test_table")
    db.upsert(table="test_table", primary_key="id", data=dict(id=6, name="james", job="chef"))
    db.commit("test_table", force=True)

    result = db.sql("select * from test_table", dtype="polars")
    assert result.shape == (2, 3)

def test_commit_and_versioning(db):
    db.upsert(table="test_table", primary_key="id", data=dict(id=5, name="edward"))
    db.commit("test_table")

    db.upsert(table="test_table", primary_key="id", data=dict(id=5, name="henry"))

    result = db.sql("select * from test_table", dtype="polars")
    assert result.shape == (1, 2)
    assert result["id"].to_list() == [5]
    assert result["name"].to_list() == ["henry"]

    db.checkout(table="test_table", version=0)

    result = db.sql("select * from test_table", dtype="polars")
    assert result.shape == (1, 2)
    assert result["id"].to_list() == [5]
    assert result["name"].to_list() == ["edward"]

def test_delete_table(db):
    db.upsert(table="test_table", primary_key="id", data=dict(id=5, name="george"))
    db.commit("test_table")

    result = db.sql("select * from test_table", dtype="polars")
    
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