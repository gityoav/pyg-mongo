from pyg_base import wrapper, getargs, as_list, getcallargs, cell_item, logger
from pyg_mongo._table import mongo_table
from pyg_mongo._sql_table import get_sql_table
from pyg_mongo._encoders import root_path
from pyg_mongo._periodic_cell import periodic_cell
from functools import partial
import pandas as pd
from pyg_npy import pd_read_npy

class cell_cache(wrapper):
    """
    Rather than write this:

    >>> def function(a, b):
    >>>     return a + b        

    >>> db = partial(mongo_table, db = 'db', table = 'table', pk = ['key'])
    >>> periodic_cell(function, a =  1, b = 2, db = db, key = 'my key')
        
    You can write this:
        
    >>> f = cell_cache(function, db = 'db', table = 'table', pk = 'key')
    >>> f(a = 1, b = 2, key = 'my key')

    If we are interested just in values...
    
    >>> f = db_cache(function, db = 'db', table = 'table', pk = 'key')
    >>> assert f(a = 1, b = 2, key = 'my key') == 3

    :Parameters:
    ------------
    pk: str/list
        primary keys of the table, using the keyword arguments of the function. If missing, uses all keywords

    db: str
        name of database where data is to be stored

    table: str
        name of table/collection where data is stored. If not provided, defaults to the function's name

    url: str
        location of mongodb server

    server: str
        location of sql server

    cell: cell
        type of cell to use when caching the data

    cell_kwargs: dict
        parameters for the cell determining its operation, e.g. for periodic_cell, the periodicity
        
    external:
        list of parameters that are part of the primary keys but are not part of the function args

    :Example:
    ---------
    >>> from pyg import *
    >>> @db_cache
    >>> def f(a,b):
    >>>     return a+b
    
    >>> assert f(1,2) == 3

    >>> @cell_cache
    >>> def f(a,b):
    >>>     return a+b
    >>> assert f(1,2).data == 3

    >>> @cell_cache(pk = 'key')
    >>> def f(a,b):
    >>>     return a+b
    >>> f(1,2, key = 'key', go = 1)

    
    """
    def __init__(self, function = None, db = 'cache', table = None, url = None, server = None, pk = None, cell = periodic_cell, writer = None, cell_kwargs = None, external = None):
        cell_kwargs  = cell_kwargs or {}
        super(cell_cache, self).__init__(function = function, pk = pk, db = db, table = table, url = url, server = server, cell = cell, writer = writer, cell_kwargs = cell_kwargs, external = external)
        if hasattr(function, 'output'):
            self.output = function.output

    @property
    def _pk(self):
        if self.pk is None and self.function is not None:
            args = getargs(self.function)
            self.pk = args
        return as_list(self.pk)

    @property
    def _external(self):
        if self.external is None and self.function is not None:
            args = getargs(self.function)
            self.external = [key for key in self._pk if key not in args]
        return self.external

    @property
    def _table(self):
        if self.table is None and self.function is not None:
            return self.function.__name__
        else:
            return self.table
    
    @property
    def _db(self):
        if self.server is None:    
            return partial(mongo_table, table = self._table, db = self.db, pk = self._pk, url = self.url, writer = self.writer)
        else:
            return partial(get_sql_table, table = self._table, db = self.db, pk = self._pk, server = self.server, writer = self.writer)
            
    
    def _external_kwargs(self, args, kwargs):
        external = self._external
        external_kwargs = {key : value for key, value in kwargs.items() if key in external}
        kwargs = {key : value for key, value in kwargs.items() if key not in external}
        external_kwargs.update(self.cell_kwargs)
        external_kwargs.update(kwargs)
        if args:
            callargs = getcallargs(self.function, *args, **kwargs)
            external_kwargs.update(callargs)
        return external_kwargs
    
    def wrapped(self, *args, **kwargs):
        db = self._db
        go = kwargs.pop('go',0)
        mode = kwargs.pop('mode',0)
        external_kwargs = self._external_kwargs(args, kwargs)
        res = self.cell(self.function, db = db, **external_kwargs).load(mode = mode)
        try:
            res = res.go(go = go)
        except Exception:
            logger.warning('Unable to run cell, returning cached data')
        return res

class db_cache(cell_cache):
    def wrapped(self, *args, **kwargs):
        res = super(db_cache, self).wrapped(*args, **kwargs)
        return cell_item(res)
            


