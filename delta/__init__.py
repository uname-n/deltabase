from polars import SQLContext, DataFrame, LazyFrame, read_delta, coalesce, struct
from os.path import join, exists
from os import listdir
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
                delta_cls.__delta_sql_context.register(table, read_delta(table_path))
        
        return delta_cls

    def __sync_data(self, primary_key:str, target_data:LazyFrame, source_data:DataFrame) -> DataFrame:
        target_data = target_data.collect()
        update_data = source_data.join(
            target_data,
            on=primary_key,
            how="full",
            suffix="_delta_target",
            coalesce=True
        )

        coalesce_columns = [col for col in update_data.columns if "_delta_target" in col]
        coalesce_columns_strip = [col.replace("_delta_target", "") for col in coalesce_columns]

        for col in coalesce_columns_strip:
            update_data = update_data.with_columns(
                coalesce([f"{col}_delta_target", col]).alias(col)
            )

        update_data = update_data.drop(coalesce_columns)
        return update_data

    def __add_multiple(self, table:str, primary_key:str, data:list[dict]):
        if table not in self.tables:
            target_data = DataFrame(data)
            self.__delta_sql_context.register(table, target_data)
            return target_data
        
        table_path = join(self.__delta_source, table)
        if exists(table_path):
            source_data = read_delta(table_path)
            staged_data = self.sql(f"select * from {table}").lazy()
            source_data = self.__sync_data(primary_key, staged_data, source_data)
        else:
            source_data = self.sql(f"select * from {table}").lazy()

        target_data = DataFrame(data).lazy()
        update_data = self.__sync_data(primary_key, target_data, source_data)

        self.__delta_sql_context.register(table, update_data)

        return update_data

    def add(self, table:str, primary_key:str, data:list[dict] | dict):
        if type(data) == dict: return self.__add_multiple(table, primary_key, [data])
        elif type(data) == list: return self.__add_multiple(table, primary_key, data)
        else: raise ValueError(f"'data' was provided '{type(data)}', type must be 'list[dict]' or 'dict'")

    def delete(self, table:str, filter:callable) -> int:
        source_data:DataFrame = self.sql(f"select * from {table}")     
        filter_data = source_data.filter(
            ~struct(source_data.columns).map_elements(filter, return_dtype=bool)
        )
        self.__delta_sql_context.register(table, filter_data)
        return len(source_data) - len(filter_data)
        
    def commit(self, table:str, overwrite:bool=False, force:bool=False):
        data = self.sql(f"select * from {table}")
        options = dict(mode="overwrite")
        if force: options["delta_write_options"] = {"schema_mode":"overwrite"}
        return data.write_delta(join(self.__delta_source, table), **options)
    
    def checkout(self, table:str, version:int|str|datetime):
        table_path = join(self.__delta_source, table)
        self.__delta_sql_context.register(table, read_delta(table_path, version=version))

    def sql(self, query:str) -> DataFrame:
        return self.__delta_sql_context.execute(query).collect()
