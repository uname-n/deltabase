try:
    from IPython.core.magic import Magics, magics_class, cell_magic
    from IPython import get_ipython
except ImportError:
    raise ImportError("NotAWizardError: install `pip install deltabase[magic]` to use magic.")

from . import delta

@magics_class
class magic (Magics):
    def __init__(self, shell, delta:delta):
        super(magic, self).__init__(shell)
        self.delta = delta

    @cell_magic
    def sql(self, line, cell):
        return self.delta.sql(query=cell, dtype="polars")

def enable(delta:delta):
    ipython = get_ipython()
    if ipython: ipython.register_magics(magic(ipython, delta))