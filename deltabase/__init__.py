#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright 2024  darryl mcculley

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
from typing import Any, TypeVar, Type

from polars import SQLContext, DataFrame, LazyFrame, Schema, sql_expr, scan_delta, struct, coalesce, from_dicts, from_dict, from_pandas
from polars.exceptions import SchemaError

from deltalake import WriterProperties
from datetime import datetime
from os.path import exists, isdir, join
from os import listdir

from deltalake.exceptions import TableNotFoundError

from logging import getLogger

debugger = getLogger("deltabase")

T = TypeVar("T", bound="delta")

class delta_config:
    dtype:str="json" 
    writer_properties:WriterProperties = WriterProperties()
    ai_model:str="gpt-4o-mini"

class delta:
    __delta_source:str
    __delta_sql_context:SQLContext=SQLContext(frames=[])
    __delta_sql_context_schema:dict[str, Schema]={}
    config:delta_config
    
    @property
    def tables(self):
        """ list all tables within the sql context.

            returns a list of table names available in the sql context.

            >>> db.tables  # output: ["table_1", "table_2"]
        """
        return self.__delta_sql_context.tables()

    @classmethod
    def connect(cls: Type[T], path:str, config:delta_config=delta_config(), scan_local_dir:bool=True) -> T:
        """ connects to a remote source if provided, or local path, sets config, and automatically scans for tables.

            **args**:
                - **path**: the file path or uri to connect to, can be a local directory or remote storage.
                - **config**: `optional` configuration settings for the delta instance. default is an instance of `delta_config`.
                - **scan_local_dir**: `optional` automatically scan for tables when a local directory is provided. default is `true`
                
            >>> db = delta.connect(path="local_path/mydelta")
            >>> db = delta.connect(path="az://<container>/<path>")
            >>> db = delta.connect(path="s3://<bucket>/<path>")
            >>> db = delta.connect(path="gs://<bucket>/<path>")
        """
        delta_cls = cls()
        delta_cls.__delta_source = path
        delta_cls.config = config

        try: from .magic import enable; enable(delta_cls)
        except ImportError as e: pass

        if not exists(path) or "://" in path or not scan_local_dir: return delta_cls

        for database in listdir(delta_cls.__delta_source):
            path = join(delta_cls.__delta_source, database)
            if isdir(path):
                for table in listdir(join(delta_cls.__delta_source, database)):
                    if not table.startswith("."):
                        table_path = join(delta_cls.__delta_source, database, table)
                        if exists(table_path):
                            try: delta_cls.register(database=database, table=table)
                            except Exception as e: raise e

        return delta_cls

    def register(self, 
        table:str, 
        pyarrow_options:dict=None, 
        alias:str=None,
        database:str="default",
        version:int|str|datetime=None,
        data:DataFrame|LazyFrame=None,
    ) -> Exception:
        """ registers the provided data, or loads the table from the delta source if no data is provided.

            **args**:
            - **table**: the name of the table to register.
            - **pyarrow_options**: `optional` options for loading the table using pyarrow.
            - **alias**: `optional` an alias to use for the table within the sql context.
            - **database**: `optional` the name of the database where the table is located. default is `'default'`.
            - **version**: `optional` the version of the table to load, can be an integer, string, or datetime.
            - **data**: `optional` a `DataFrame` or `LazyFrame` to register instead of loading from the delta source.

            >>> db.register(database="mydatabase", table="mytable", data=...)
            >>> db.register(database="mydatabase", table="mytable", version=1)
            >>> db.register(database="mydatabase", table="mytable", alias="mydatabase_mytable")
            >>> db.register(database="mydatabase", table="mytable", pyarrow_options={
            >>>     "partitions": [("year", "=", "2021")]
            >>> })
        """
        table_path = join(self.__delta_source, database, table)

        options = dict()
        if pyarrow_options: 
            options["use_pyarrow"]=True
            options["pyarrow_options"] = pyarrow_options
        if isinstance(version, int|str|datetime): options["version"] = version
        table_name = alias if alias else table

        try:
            if data is None: data = scan_delta(table_path, **options)
            elif not isinstance(data, (DataFrame, LazyFrame)): 
                raise TypeError(f"deltabase.register:: provided {type(data)} is not {DataFrame} or {LazyFrame}")
            self.__delta_sql_context.register(table_name, data)
            self.__delta_sql_context_schema[table_name] = data.collect_schema()
        except (TableNotFoundError, FileNotFoundError) as e: return e
    
    def __sync_data(self, primary_key:str, target_data:LazyFrame, source_data:LazyFrame) -> LazyFrame:
        """ performs a full outer join on the primary key and coalesces data to ensure consistency.
            
            **args**:
            - **primary_key**: the primary key on which the join will be performed.
            - **target_data**: the target `LazyFrame` containing the data to be updated.
            - **source_data**: the source `LazyFrame` containing the data to synchronize with.

            returns a lazyframe containing the synchronized data.
        """
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

    def upsert(self, 
        table:str,
        primary_key:str,
        data:list[dict] | dict | DataFrame | LazyFrame,
        database:str="default",
    ) -> Exception:
        """ updates or inserts records in the specified table, with schema changes handled automatically. changes are reflected in the sql context, but a commit is required to persist them.

            **args**:
            - **table**: the name of the table to upsert data into.
            - **primary_key**: the primary key used to match records for updates.
            - **data**: the data to be upserted, can be a list of dictionaries, a dictionary, `DataFrame`, or `LazyFrame`.
            - **database**: `optional` the name of the database where the table is located. default is `'default'`.   

            >>> db.upsert(database="mydatabase", table="mytable", primary_key="id", data=...)
        """
        if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict): data = from_dicts(data).lazy()
        elif isinstance(data, dict): data = from_dict(data).lazy()
        elif isinstance(data, DataFrame): data = data.lazy()
        elif isinstance(data, LazyFrame): pass
        else: return ValueError(f"'data' was provided as '{type(data)}', type must be 'list[dict]' | 'dict' | 'DataFrame' | 'LazyFrame'")

        if table not in self.tables:
            return self.register(database=database, table=table, data=data)

        table_path = join(self.__delta_source, database, table)
        
        try: 
            source_data = scan_delta(table_path)
            staged_data = self.sql(f"select * from {table}", lazy=True)
            source_data = self.__sync_data(primary_key, staged_data, source_data)
        except (TableNotFoundError, FileNotFoundError) as e:
            source_data = self.sql(f"select * from {table}", lazy=True)

        update_data = self.__sync_data(primary_key, data, source_data)
        
        return self.register(database=database, table=table, data=update_data)
    
    def delete(self, table:str, filter:str|LambdaType="*", database:str="default") -> Exception:
        """ removes records using a specified sql condition or lambda function. this only affects the sql context and does not delete data from disk or cloud storage.

            **args**:
            - **table**: the name of the table from which records will be deleted.
            - **filter**: `optional` a sql condition string or lambda function to filter the records to delete. default is `'*'`, which deletes all records.
            - **database**: `optional` the name of the database where the table is located. default is `'default'`.

            >>> db.delete(database="mydatabase", table="mytable")
            >>> db.delete(database="mydatabase", table="mytable", filter="name='bob'")
            >>> db.delete(database="mydatabase", table="mytable", filter=lambda row: row["name"] == "bob")
        """
        table_path = join(self.__delta_source, database, table)
        if filter == "*": 
            self.__delta_sql_context.unregister(table_path)
            return None
        elif isinstance(filter, str):
            source_data = self.sql(f"select * from {table}", lazy=True)
            filter_data = source_data.filter(~sql_expr(filter))
        elif type(filter) == LambdaType:
            source_data = self.sql(f"select * from {table}", lazy=True)
            filter_data = source_data.filter(
                ~struct(source_data.collect_schema().names()).map_elements(filter, return_dtype=bool)
            )
        else: 
            return ValueError(f"'filter' was provided as '{type(filter)}', type must be 'callable' or 'str'")

        return self.register(database=database, table=table, data=filter_data)

    def sql(self, query:str, lazy:bool=False, dtype:str=None) -> DataFrame | LazyFrame:
        """ executes the provided sql query and returns the result as a dataframe or lazyframe. the result type can be specified via the dtype argument.

            **args**:
            - **query**: the sql query to execute.
            - **lazy**: `optional` returns a lazyframe if set to true. default is `false`.
            - **dtype**: `optional` sets the output data type. default is `'json'`.

            >>> db.sql("select * from mytable")
        """
        dtype = dtype if dtype else self.config.dtype 
        if lazy: return self.__delta_sql_context.execute(query)
        try: data:DataFrame = self.__delta_sql_context.execute(query).collect()
        except SchemaError as e: data:DataFrame = DataFrame(
                schema=self.__delta_sql_context.execute(query).collect_schema()
            )
        match dtype:
            case "polars": return data
            case "json": return data.to_dicts()
            case _: raise ValueError(f"'dtype' was provided as '{dtype}', type must be one of the following ['polars', 'json']")

    def commit(self, 
        table:str,
        force:bool=False,
        partition_by:list[str]=None,
        database:str="default",
    ) -> Exception:
        """ persists the current state of a table in the sql context to the delta source, with optional schema or partitioning options.

            **args**:
            - **table**: the name of the table to commit.
            - **force**: `optional` force schema changes during the commit.
            - **partition_by**: `optional` list of fields to partition by.
            - **database**: `optional` name of the database. default is `'default'`.

            >>> db.commit(database="mydatabase", table="mytable")
            >>> db.commit(database="mydatabase", table="mytable", force=True)
            >>> db.commit(database="mydatabase", table="mytable", partition_by=["job"])
        """
        table_path = join(self.__delta_source, database, table)
        data = self.sql(f"select * from {table}", dtype="polars")
        
        options = {"mode":"overwrite"}
        options.setdefault("delta_write_options", {})
        options["delta_write_options"]["writer_properties"] = self.config.writer_properties

        if partition_by: options["delta_write_options"]["partition_by"] = partition_by
        if force: options["delta_write_options"]["schema_mode"] = "overwrite"
        
        try: data.write_delta(table_path, **options)
        except Exception as e: return e

    def checkout(self, table:str, version:int|str|datetime, database:str="default") -> Exception:
        """ reloads a previous version of a table from the delta source into the sql context.

            **args**:
            - **table**: the name of the table to revert.
            - **version**: the version to checkout, which can be an integer, string, or datetime.
            - **database**: `optional` name of the database. default is `'default'`.
        
            >>> db.checkout(database="mydatabase", table="mytable", version=1)
        """
        return self.register(database=database, table=table, version=version)
    
    def schema(self, table:str) -> Schema|None:
        """ reloads a previous version of a table from the delta source into the sql context.

            **args**:
            - **table**: the name of the table.
            - **database**: `optional` name of the database. default is `'default'`.
        
            >>> db.schema(table="mytable")
        """
        schema = self.__delta_sql_context_schema.get(table)
        if schema: return schema.to_python()
        return None
