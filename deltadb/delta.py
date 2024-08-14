#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright 2024  Darryl McCulley

#     This program is free software: you can redistribute it and/or modify
#     it under the terms of the GNU General Public License as published by
#     the Free Software Foundation, either version 3 of the License, or
#     any later version.

#     This program is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU General Public License for more details.

#     You should have received a copy of the GNU General Public License
#     along with this program.  If not, see <https://www.gnu.org/licenses/>.

from types import LambdaType

from polars import SQLContext, DataFrame, LazyFrame, sql_expr, scan_delta, coalesce, struct
from os.path import join, exists
from os import listdir
from shutil import rmtree
from datetime import datetime

class delta:
    __delta_source:str
    __delta_sql_context:SQLContext=SQLContext(frames=[])

    @property
    def tables(self) -> list[str]:
        """ list tables in the sql context.

            **return**:
                - **tables**: a list of table names

            >>> db = delta.connect("my.db")
            >>> tables = db.tables
        """
        return self.__delta_sql_context.tables()

    @classmethod
    def connect(cls, path:str):
        """ connect to an existing database or create a new one.

            **args**:
                - **path**: path to the database

            **return**:
                - **delta**: a new or existing database instance

            >>> db = delta.connect("my.db")
        """
        delta_cls = cls()
        delta_cls.__delta_source = path

        if not exists(path): return delta_cls

        for table in listdir(delta_cls.__delta_source):
            table_path = join(delta_cls.__delta_source, table)
            if exists(table_path):
                delta_cls.__delta_sql_context.register(table, scan_delta(table_path))
        
        return delta_cls
    
    def register(self, table:str, pyarrow_options:dict):
        table_path = join(self.__delta_source, table)
        self.__delta_sql_context.register(table, scan_delta(table_path, pyarrow_options=pyarrow_options))

    def __sync_data(self, primary_key:str, target_data:LazyFrame, source_data:LazyFrame) -> LazyFrame:
        """sync data between target and source based on the primary key."""
        update_data = source_data.join(
            target_data,
            on=primary_key,
            how="full",
            suffix="_delta_target",
            coalesce=True
        )

        update_data_columns = update_data.collect_schema().names()

        coalesce_columns = [col for col in update_data_columns if "_delta_target" in col]
        coalesce_columns_strip = [col.replace("_delta_target", "") for col in coalesce_columns]

        update_data = update_data.with_columns([
            coalesce([f"{col}_delta_target", col]).alias(col) 
            if f"{col}_delta_target" in update_data_columns else update_data[col]
            for col in coalesce_columns_strip
        ]).drop(coalesce_columns)
        
        return update_data
    
    def __upsert_multiple(self, table:str, primary_key:str, data:LazyFrame) -> bool:
        """upsert multiple records into the specified table."""
        if table not in self.tables:
            self.__delta_sql_context.register(table, data)
            return True
        
        table_path = join(self.__delta_source, table)
        if exists(table_path):
            source_data = scan_delta(table_path)
            staged_data = self.sql(f"select * from {table}", lazy=True)
            source_data = self.__sync_data(primary_key, staged_data, source_data)
        else:
            source_data = self.sql(f"select * from {table}", lazy=True)

        update_data = self.__sync_data(primary_key, data, source_data)

        self.__delta_sql_context.register(table, update_data)
        return True

    def upsert(self, table:str, primary_key:str, data:list[dict] | dict | DataFrame | LazyFrame) -> bool:
        """ upsert single or multiple records with automatic schema management. the sql context is updated immediately, but a commit must be made to persist on disk.

            **args**:
                - **table**: name of the table
                - **primary_key**: name of the primary key in the records
                - **data**: records to upsert; can be a list of dictionaries, a single dictionary, a DataFrame, or a LazyFrame

            **return**:
                - **success**: true if the operation is successful

            >>> db = delta.connect("my.db")
            >>> db.upsert(table="mytable", primary_key="id", data=dict(id=1, name="bob"))
        """
        if type(data) == dict: data = LazyFrame([data])
        elif type(data) == list: data = LazyFrame(data)
        elif type(data) == DataFrame: data = data.lazy()
        elif type(data) == LazyFrame: data = data
        else: raise ValueError(f"'data' was provided as '{type(data)}', type must be 'list[dict]' or 'dict'")
        return self.__upsert_multiple(table, primary_key, data)

    def delete(self, table:str, filter:str|LambdaType=None) -> bool:
        """ delete an entire table or records from a table using a sql condition or lambda function filter. (sql is much faster)

            **args**:
                - **table**: name of the table
                - **filter**: sql condition or lambda function

            **return**:
                - **success**: true if the operation is successful

            >>> db = delta.connect("my.db")
            >>> db.delete(table="mytable", filter="name='bob'")
            >>> db.delete(table="mytable", filter=lambda row: row["name"] == "bob")
            >>> db.delete(table="mytable")
        """
        if not filter: 
            self.__delta_sql_context.unregister(table)
            return
        elif type(filter) == str:
            source_data = self.sql(f"select * from {table}", lazy=True)
            filter_data = source_data.filter(~sql_expr(filter))
        elif type(filter) == LambdaType:
            source_data = self.sql(f"select * from {table}", lazy=True)
            filter_data = source_data.filter(
                ~struct(source_data.collect_schema().names()).map_elements(filter, return_dtype=bool)
            )
        else: raise ValueError(f"'filter' was provided as '{type(filter)}', type must be 'callable' or 'str'")

        self.__delta_sql_context.register(table, filter_data)
        return True

    def commit(self, table:str, force:bool=False, partition_by:list[str]=None) -> bool:
        """ commit a table's current sql context to a delta table.

            **args**:
                - **table**: name of the table
                - **force**: allows changes to the delta table schema. default: false

            **return**:
                - **success**: true if the operation is successful

            >>> db = delta.connect("my.db")
            >>> db.upsert(table="mytable", primary_key="id", data=dict(id=1, name="bob"))
            >>> db.commit("mytable")
        """
        table_path = join(self.__delta_source, table)
        if table not in self.tables: 
            if exists(table_path): 
                rmtree(table_path)
                return
            else:
                raise AssertionError(f"table, '{table}' was not found.")
        data = self.sql(f"select * from {table}", dtype="polars")
        options = dict(mode="overwrite", delta_write_options=dict())

        if partition_by: options["delta_write_options"]["partition_by"] = partition_by
        if force: options["delta_write_options"]["schema_mode"] = "overwrite"

        data.write_delta(table_path, **options)
        return True
    
    def checkout(self, table:str, version:int|str|datetime) -> bool:
        """ revert a table to a previous commit.

            **args**:
                - **table**: name of the table
                - **version**: datetime or version number, starting at 0

            **return**:
                - **success**: true if the operation is successful

            >>> db = delta.connect("my.db")
            >>> db.checkout("mytable", version=0)
        """
        table_path = join(self.__delta_source, table)
        self.__delta_sql_context.register(table, scan_delta(table_path, version=version))
        return True

    def sql(self, query:str, lazy:bool=False, dtype:str="json") -> DataFrame | LazyFrame:
        """ query data from any delta table.

            **args**:
                - **query**: sql statement to execute
                - **dtype**: set the desired output dtype. options: ['polars', 'json']. default: 'json'

            **return**:
                - **data**: output of the query result

            >>> db = delta.connect("my.db")
            >>> db.sql("select * from mytable")
        """
        if lazy: return self.__delta_sql_context.execute(query)

        data:DataFrame = self.__delta_sql_context.execute(query).collect()
        match dtype:
            case "polars": return data
            case "json": return data.to_dicts()
            case _: raise ValueError(f"'dtype' was provided as '{dtype}', type must be one of the following ['polars', 'json']")
        