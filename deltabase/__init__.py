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
from typing import TypeVar, Type

from polars import SQLContext, DataFrame, LazyFrame, sql_expr, scan_delta, struct, coalesce, from_dicts, from_dict
from datetime import datetime
from os.path import exists, join
from os import listdir

from deltalake.exceptions import TableNotFoundError

T = TypeVar("T", bound="delta")

class delta_config:
    dtype:str="json" 

class delta:
    __delta_source:str
    __delta_sql_context:SQLContext=SQLContext(frames=[])
    config:delta_config

    @property
    def tables(self):
        """ list tables in the sql context. 
        """
        return self.__delta_sql_context.tables()

    @classmethod
    def connect(cls: Type[T], path:str, config:delta_config=delta_config()) -> T:
        """ returns `delta` class, if `path` is local tables will be loaded. 
        """
        delta_cls = cls()
        delta_cls.__delta_source = path
        delta_cls.config = config

        if not exists(path) or "://" in path: return delta_cls
        
        for database in listdir(delta_cls.__delta_source):
            print(database)
            for table in listdir(join(delta_cls.__delta_source, database)):
                print(table)
                table_path = join(delta_cls.__delta_source, database, table)
                if exists(table_path):
                    print(table_path)
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
        """ register data to the local sql context. 
        """
        table_path = join(self.__delta_source, database, table)
        options = dict()
        if pyarrow_options: options["pyarrow_options"] = pyarrow_options
        if isinstance(version, int|str|datetime): options["version"] = version
        try: self.__delta_sql_context.register(alias if alias else table, data if isinstance(data, (DataFrame, LazyFrame)) else scan_delta(table_path, **options))
        except (TableNotFoundError, FileNotFoundError) as e: return e
    
    def __sync_data(self, primary_key:str, target_data:LazyFrame, source_data:LazyFrame) -> LazyFrame:
        """ sync data between target and source based on the primary key. 
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
        """ upsert single or multiple records with automatic schema management. 
            
            the sql context is updated immediately, but a commit must be made to persist. 
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
        """ delete an entire table or records from a table using a sql condition or lambda function filter. (sql is much faster) 
            
            only deletes from sql context, files on disk or in cloud are not affected.
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
        """ query data from any delta table in sql context. 
        """
        dtype = dtype if dtype else self.config.dtype 
        if lazy: return self.__delta_sql_context.execute(query)
        data:DataFrame = self.__delta_sql_context.execute(query).collect()
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
        """ commit a table's current sql context to a delta table. 
        """
        table_path = join(self.__delta_source, database, table)
        data = self.sql(f"select * from {table}", dtype="polars")
        
        options = dict(mode="overwrite", delta_write_options=dict())
        if partition_by: options["delta_write_options"]["partition_by"] = partition_by
        if force: options["delta_write_options"]["schema_mode"] = "overwrite"

        try: data.write_delta(table_path, **options)
        except Exception as e: return e

    def checkout(self, table:str, version:int|str|datetime, database:str="default") -> Exception:
        """ revert a table to a previous commit. 
        """
        return self.register(database=database, table=table, version=version)