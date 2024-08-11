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
    def tables(self):
        return self.__delta_sql_context.tables()

    @classmethod
    def connect(cls, path:str):
        delta_cls = cls()
        delta_cls.__delta_source = path

        if not exists(path): return delta_cls

        for table in listdir(delta_cls.__delta_source):
            table_path = join(delta_cls.__delta_source, table)
            if exists(table_path):
                delta_cls.__delta_sql_context.register(table, scan_delta(table_path))
        
        return delta_cls
    
    def __sync_data(self, primary_key:str, target_data:LazyFrame, source_data:LazyFrame) -> LazyFrame:
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
    
    def __upsert_multiple(self, table:str, primary_key:str, data:list[dict]) -> bool:
        if table not in self.tables:
            target_data = LazyFrame(data)
            self.__delta_sql_context.register(table, target_data)
            return target_data
        
        table_path = join(self.__delta_source, table)
        if exists(table_path):
            source_data = scan_delta(table_path)
            staged_data = self.sql(f"select * from {table}", lazy=True)
            source_data = self.__sync_data(primary_key, staged_data, source_data)
        else:
            source_data = self.sql(f"select * from {table}", lazy=True)

        target_data = LazyFrame(data)
        update_data = self.__sync_data(primary_key, target_data, source_data)

        self.__delta_sql_context.register(table, update_data)
        return True

    def upsert(self, table:str, primary_key:str, data:list[dict] | dict):
        if type(data) == dict: return self.__upsert_multiple(table, primary_key, [data])
        elif type(data) == list: return self.__upsert_multiple(table, primary_key, data)
        else: raise ValueError(f"'data' was provided as '{type(data)}', type must be 'list[dict]' or 'dict'")

    def delete(self, table:str, filter:str|LambdaType=None) -> bool:
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

    def commit(self, table:str, force:bool=False) -> bool:
        table_path = join(self.__delta_source, table)
        if table not in self.tables: 
            if exists(table_path): 
                rmtree(table_path)
                return
            else:
                raise AssertionError(f"table, '{table}' was not found.")
        data = self.sql(f"select * from {table}", dtype="polars")
        options = dict(mode="overwrite")
        if force: options["delta_write_options"] = {"schema_mode":"overwrite"}
        data.write_delta(table_path, **options)
        return True
    
    def checkout(self, table:str, version:int|str|datetime) -> bool:
        table_path = join(self.__delta_source, table)
        self.__delta_sql_context.register(table, scan_delta(table_path, version=version))
        return True

    def sql(self, query:str, lazy:bool=False, dtype:str="json") -> DataFrame | LazyFrame:
        if lazy: return self.__delta_sql_context.execute(query)

        data:DataFrame = self.__delta_sql_context.execute(query).collect()
        match dtype:
            case "polars": return data
            case "json": return data.to_dicts()
            case _: raise ValueError(f"'dtype' was provided as '{dtype}', type must be one of the following ['polars', 'json']")
