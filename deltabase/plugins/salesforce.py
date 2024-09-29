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

from .base import delta_plugin

from os import environ as env
from re import search, findall

from pandas import DataFrame
from simple_salesforce import Salesforce, format_soql
from polars import from_pandas

class salesforce (Salesforce, delta_plugin):
    format = format_soql
    select = lambda object_name, select: "SELECT {select} FROM {object_name}".format(select=", ".join(select), object_name=object_name) 
    where = lambda field, match: f" WHERE {field} IN {format_soql('{}', match)}"

    def query_all_as_dataframe(self, 
        query:str, 
        include_deleted:bool=False,
        include_parent_relationship:bool=False,
        include_attributes:bool=False,
        **kwargs
    ):
        query = query.replace("\n", " ").replace("  ", " ").strip()
        attributes = search(r"(?<=SELECT|select)(.*)(?=FROM|from)", query).group() # type: ignore
        relationships = findall(r"([0-9A-z]+\.[0-9A-z]+)", attributes)
        columns = [col.replace(".", "_") for col in findall(r"([0-9A-z_.]+)", attributes)]

        records = list(self.query_all_iter(query, include_deleted=include_deleted, **kwargs))
        
        dataframe = DataFrame(records)
        if len(dataframe) == 0: return DataFrame(records, columns=columns)

        rm = []
        for relationship in relationships:
            object_name, attr = relationship.split(".")
            
            dataframe[f"{object_name}_{attr}"] = dataframe[object_name].apply(lambda x: x if x == None else x[attr])
            if object_name not in rm: rm.append(object_name)

        if not include_attributes: dataframe = dataframe.drop(columns=["attributes"])
        if not include_parent_relationship: dataframe = dataframe.drop(columns=rm)
        
        return dataframe
    
    @classmethod
    def register(
        cls,
        delta,
        table:str,
        query:str,
    ):
        """ registers the result of a soql query from salesforce as a table into the local sql context.

            **args**:
                - **delta**: delta instance.
                - **table**: the name of the table to register.
                - **query**: soql query to retrieve salesforce data.
                
            >>> db.salesforce.register(delta=db, table="salesforce_opportunity", query="select Id, CreatedDate from Opportunity")
        """
        client = cls(
            username=env["SALESFORCE_USERNAME"],
            password=env["SALESFORCE_PASSWORD"],
            security_token=env["SALESFORCE_SECURITY_TOKEN"],
        )
        data = client.query_all_as_dataframe(query)
        data = from_pandas(data)
        delta.register(table=table, data=data)
        return data