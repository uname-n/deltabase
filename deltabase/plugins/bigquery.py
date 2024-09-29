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

from google.cloud import bigquery as google_bigquery
from polars import from_arrow

class bigquery (delta_plugin):
    @classmethod
    def register(
        cls,
        delta,
        table:str,
        query:str,
    ):
        """ registers the result of a soql query from bigquery as a table into the local sql context.

            **args**:
                - **delta**: delta instance.
                - **table**: the name of the table to register.
                - **query**: soql query to retrieve bigquery data.
                
            >>> db.bigquery.register(
            >>>     delta=db, table="mydata", 
            >>>     query='''
            >>>         select name from `bigquery-public-data.usa_names.usa_1910_2013`
            >>>         where state = "TX" 
            >>>         limit 100
            >>>     '''
            >>> )
        """
        client = google_bigquery.Client()
        query_job = client.query(query)
        rows = query_job.result()

        data = from_arrow(rows.to_arrow())

        delta.register(table=table, data=data)
        return data