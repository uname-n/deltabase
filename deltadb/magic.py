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

from IPython.core.magic import Magics, magics_class, line_magic, cell_magic

from deltadb.delta import delta

@magics_class
class delta_magic (Magics):
    def __init__(self, shell):
        super(delta_magic, self).__init__(shell)
        self.db = None

    @line_magic
    def connect(self, line:str):
        self.db = delta.connect(line)

    @line_magic
    def tables(self, line:str) -> list[str]:
        if not self.db: return "connect to a database first. %connect path/to/database"
        return self.db.tables
    
    @line_magic
    def register(self, line:str) -> list[str]:
        if not self.db: return "connect to a database first. %connect path/to/database"
        args = line.split("/")
        assert len(args) == 2, "invalid. %register {delta_source}/{table}"
        delta_source, table = args
        return self.db.register(table=table, delta_source=delta_source)

    @cell_magic
    def sql(self, line:str, cell:str):
        if not self.db: return "connect to a database first. %connect path/to/database"
        return self.db

def load_ipython_extension(ipython):
    delta = delta_magic(ipython)
    ipython.register_magics(delta)
