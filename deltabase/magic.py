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

try:
    from IPython.core.magic import Magics, magics_class, cell_magic, line_magic
    from IPython.display import Markdown, display
    from IPython import get_ipython
except ImportError:
    raise ImportError("`deltabase[magic]` package required for magic.")

from . import delta

from json import dumps

@magics_class
class magic (Magics):
    def __init__(self, shell, delta:delta):
        super(magic, self).__init__(shell)
        self.__openai_chat_history = []
        self.delta = delta

    @cell_magic
    def sql(self, line, cell):
        data = self.delta.sql(query=cell, dtype="polars")

        args = line.split(" ")
        if "--table" in line and "--key" in line: 
            table = args[args.index("--table")+1]
            key = args[args.index("--key")+1]
            if "--database" in line: database = args[args.index("--upsert")+1]
            else: database = "default"
            self.delta.upsert(database=database, table=table, primary_key=key, data=data)

        return data
    
    @cell_magic
    def ai(self, line, cell):        
        try: from openai import OpenAI
        except: raise ImportError("`deltabase[ai]` package required for `ai` magic.")
        client = OpenAI()

        context = ""
        for table in self.delta.tables:
            schema = self.delta.schema(table=table)
            if schema: context += f"- {table}: {schema}\n"

        messages = [
            {"role": "system", "content": (
                "answer the user's question. "
                "below is the data they have access to."
                "data can be accessed via sql.\n"
            )},
            {"role": "system", "content": f"here is the data available to the user.\n" + context}
        ]

        for question, answer in self.__openai_chat_history:
            messages.append({"role": "user", "content":question})
            messages.append({"role": "assistant", "content":answer})
        messages.append({"role": "user", "content":cell})

        if "--debug" in line: return display(Markdown(f"```json\n{dumps(messages, indent=4)}\n```"))
        
        completion = client.chat.completions.create(
            model=self.delta.config.ai_model,
            messages=messages
        )

        response = completion.choices[0].message.content
        self.__openai_chat_history.append((cell, response))
        return display(Markdown(response))

    @line_magic
    def ai_chat(self, line):
        match line:
            case "clear": self.__openai_chat_history = []
            case "undo": self.__openai_chat_history.pop()

def enable(delta:delta):
    ipython = get_ipython()
    if ipython: ipython.register_magics(magic(ipython, delta))