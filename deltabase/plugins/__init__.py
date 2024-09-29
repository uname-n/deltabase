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

from logging import getLogger

debugger = getLogger("deltabase.plugin.salesforce")

from .base import delta_plugin

try: from .salesforce import salesforce
except ImportError:
    debugger.info("unable to load salesforce plugin. `deltabase[salesforce]` package required.")

try: from .bigquery import bigquery
except ImportError:
    debugger.info("unable to load bigquery plugin. `deltabase[bigquery]` package required.")