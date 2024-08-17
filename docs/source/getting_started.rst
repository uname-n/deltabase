.. deltabase documentation master file, created by
   sphinx-quickstart on Fri Aug 16 20:03:37 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. toctree::
   :maxdepth: 1

Getting Started
=====================================

This guide will help you get started with DeltaBase, from installation to basic usage. By the end of this guide, you'll be able to connect to Delta tables, perform basic operations like upserts and deletes, and manage data versioning.

=====================================
Install
=====================================

To install DeltaBase, you can simply use pip:

.. code-block:: bash

    pip install deltabase


=====================================
Basic Usage
=====================================


1. Connecting to a Delta Table
-------------------------------------

.. code-block:: python

    from deltabase import delta
    db = delta.connect(path="path/to/delta")


2. Upserting Data
-------------------------------------

Once connected, you can upsert data into your tables. DeltaBase supports various data formats including dictionaries, lists of dictionaries, DataFrames, and LazyFrames:

.. code-block:: python

    data = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"}
    ]

    # Upsert data into the table 'users'
    db.upsert(table="users", primary_key="id", data=data)

This operation will automatically handle schema changes if any new fields are introduced.


3. Querying Data
-------------------------------------

After upserting data, you can query it using SQL:

.. code-block:: python

    result = db.sql("SELECT * FROM users", dtype="polars")
    print(result)

The `dtype` parameter allows you to specify the output format (e.g., JSON, Polars DataFrame).


4. Committing Changes
-------------------------------------

To persist changes to your Delta tables, you need to commit them:

.. code-block:: python

    db.commit("users")

This will write the changes to the Delta table on disk or in cloud storage.


5. Version Control
-------------------------------------

DeltaBase allows you to revert tables to previous versions easily:

.. code-block:: python

    # Revert 'users' table to version 0
    db.checkout(table="users", version=0)

This feature is particularly useful for data recovery or comparison between different data versions.


6. Deleting Data
-------------------------------------

You can also delete data using SQL conditions or lambda functions:

.. code-block:: python

    # Delete records where name is 'Alice'
    db.delete(table="users", filter="name='Alice'")

    # Or using a lambda function
    db.delete(table="users", filter=lambda row: row["name"] == "Alice")
