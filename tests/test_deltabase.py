import pytest

from deltabase import delta
from polars import DataFrame, LazyFrame

from os.path import exists
from shutil import rmtree

@pytest.fixture
def db():
    _ = delta.connect(path="test.delta")
    yield _
    for table in _.tables:
        _._delta__delta_sql_context.unregister(table)
    if exists("test.delta"): rmtree("test.delta")

def test_connect(db):
    assert isinstance(db, delta)
    assert db.tables == []

def test_add_record_dict(db):
    err = db.upsert(table="test_table", primary_key="id", data=dict(id=1, name="a"))
    assert not err, err

    err = db.upsert(table="test_table", primary_key="id", data=[
        dict(id=2, name="b"),
        dict(id=3, name="c"),
    ])
    assert not err, err
    result = db.sql("select * from test_table", dtype="polars")
    
    assert isinstance(result, DataFrame)
    assert result.shape == (3, 2)
    assert set(result["id"].to_list()) == set([1,2,3])
    assert set(result["name"].to_list()) == set(["a","b","c"])

def test_add_record_dataframe(db):
    err = db.upsert(table="test_table", primary_key="id", data=DataFrame([
        dict(id=2, name="b"),
        dict(id=3, name="c"),
    ]))
    assert not err, err
    result = db.sql("select * from test_table", dtype="polars")
    
    assert isinstance(result, DataFrame)
    assert result.shape == (2, 2)
    assert set(result["id"].to_list()) == set([2,3])
    assert set(result["name"].to_list()) == set(["b","c"])

def test_add_record_lazyframe(db):
    err = db.upsert(table="test_table", primary_key="id", data=DataFrame([
        dict(id=2, name="b"),
        dict(id=3, name="c"),
    ]).lazy())
    assert not err, err
    result = db.sql("select * from test_table", dtype="polars")
    
    assert isinstance(result, DataFrame)
    assert result.shape == (2, 2)
    assert set(result["id"].to_list()) == set([2,3])
    assert set(result["name"].to_list()) == set(["b","c"])

def test_add_mismatch_schema(db):
    err = db.upsert(table="test_table", primary_key="id", data=[
        dict(id=2, name="b", job="j"),
        dict(id=3, name="c"),
    ])
    assert not err, err
    result = db.sql("select * from test_table", dtype="polars")
    
    assert isinstance(result, DataFrame)
    assert result.shape == (2, 3)
    assert set(result["id"].to_list()) == set([2,3])
    assert set(result["name"].to_list()) == set(["b","c"])
    assert set(result["job"].to_list()) == set(["j", None])

def test_update_record(db):
    db.upsert(table="test_table", primary_key="id", data=dict(id=2, name="a"))
    db.upsert(table="test_table", primary_key="id", data=dict(id=2, name="b"))
    result = db.sql("select * from test_table", dtype="polars")

    assert isinstance(result, DataFrame)
    assert result.shape == (1, 2)
    assert set(result["id"].to_list()) == set([2])
    assert set(result["name"].to_list()) == set(["b"])

def test_update_mismatch_schema_record(db):
    db.upsert(table="test_table", primary_key="id", data=dict(id=2, name="a"))
    db.upsert(table="test_table", primary_key="id", data=[dict(id=2, name="b"), dict(id=3, name="c", job="j")])
    result = db.sql("select * from test_table", dtype="polars")

    assert isinstance(result, DataFrame)
    assert result.shape == (2, 3)
    assert set(result["id"].to_list()) == set([2, 3])
    assert set(result["name"].to_list()) == set(["b", "c"])
    assert set(result["job"].to_list()) == set([None, "j"])

def test_delete_record_sql(db):
    db.upsert(table="test_table", primary_key="id", data=dict(id=5, name="a"))
    db.upsert(table="test_table", primary_key="id", data=dict(id=6, name="b"))
    
    err = db.delete(table="test_table", filter="name='a'")
    assert not err, err

    result = db.sql("select * from test_table", dtype="polars")

    assert isinstance(result, DataFrame)
    assert result.shape == (1, 2)
    assert set(result["name"].to_list()) == set(["b"])

def test_delete_record_lambda(db):
    db.upsert(table="test_table", primary_key="id", data=dict(id=5, name="a"))
    db.upsert(table="test_table", primary_key="id", data=dict(id=6, name="b"))
    
    err = db.delete(table="test_table", filter=lambda row: row["name"] == "a")
    assert not err, err

    result = db.sql("select * from test_table", dtype="polars")

    assert isinstance(result, DataFrame)
    assert result.shape == (1, 2)
    assert set(result["name"].to_list()) == set(["b"])

def test_delete_record_wrong_type(db):
    db.upsert(table="test_table", primary_key="id", data=dict(id=5, name="a"))
    db.upsert(table="test_table", primary_key="id", data=dict(id=6, name="b"))
    
    err = db.delete(table="test_table", filter=["wrong"])
    assert isinstance(err, Exception)

def test_schema_override(db):
    db.upsert(table="test_table", primary_key="id", data=dict(id=5, name="edward"))
    db.commit("test_table")
    db.upsert(table="test_table", primary_key="id", data=dict(id=6, name="james", job="chef"))
    db.commit("test_table", force=True)

    result = db.sql("select * from test_table", dtype="polars")
    assert result.shape == (2, 3)

def test_commit_and_versioning(db):
    err = db.upsert(table="test_table", primary_key="id", data=dict(id=2, name="b"))
    assert not err, err

    err = db.commit("test_table")
    assert not err, err

    err = db.upsert(table="test_table", primary_key="id", data=dict(id=3, name="c"))
    assert not err, err

    result = db.sql("select * from test_table", dtype="polars")
    
    assert isinstance(result, DataFrame)
    assert result.shape == (2, 2)
    assert set(result["id"].to_list()) == set([2,3])
    assert set(result["name"].to_list()) == set(["b","c"])

    err = db.checkout(table="test_table", version=0)
    assert not err, err

    result = db.sql("select * from test_table", dtype="polars")
    
    assert isinstance(result, DataFrame)
    assert result.shape == (1, 2)
    assert set(result["id"].to_list()) == set([2])
    assert set(result["name"].to_list()) == set(["b"])

def test_commit_with_partitions(db):
    db.upsert(table="test_table", primary_key="id", data=LazyFrame([dict(id=1, name="alice", job="teacher")]))
    db.upsert(table="test_table", primary_key="id", data=LazyFrame([dict(id=2, name="john", job="chef")]))
    db.commit("test_table", partition_by=["job"])

    db.register(table="test_table", pyarrow_options={"partitions": [("job", "=", "chef")]})

    result = db.sql("select * from test_table", dtype="polars")

    assert isinstance(result, DataFrame)
    assert result.shape == (1, 3)
    assert result["id"].to_list() == [2]
    assert result["name"].to_list() == ["john"]

def test_register_non_existing_table(db):
    err = db.register(table="test_table")
    assert isinstance(err, Exception)

def test_json_output(db):
    db.upsert(table="test_table", primary_key="id", data=dict(id=5, name="a"))
    result = db.sql("select * from test_table", dtype="json")
    assert isinstance(result, list)
    assert isinstance(result[0], dict)
    assert result[0] == dict(id=5, name="a")