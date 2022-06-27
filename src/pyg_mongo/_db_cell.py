from pyg_base import cell, is_date, ulist, logger, cell_clear, as_list, get_DAG, get_GAD, get_cache, descendants
from pyg_base import cell_item, Dict
from pyg_mongo._q import _id, _deleted, q
from pyg_mongo._table import mongo_table
from functools import partial

# import networkx as nx

_pk = 'pk'
_db = 'db'
_updated = 'updated'
_function = 'function'

__all__ = ['db_load', 'db_save', 'db_cell', 'cell_push', 'cell_pull', 'get_cell', 'load_cell', 'get_data', 'load_data']

def get_GRAPH():
    return get_cache('GRAPH')

def is_pairs(pairs):
    """
    returns a check if the data is pairs of key-value tuples

    :Parameters:
    ------------
    pairs : tuple
        tuples of key-value tuples.

    :Returns:
    ---------
    bool
    """
    return isinstance(pairs, tuple) and min([isinstance(item, tuple) and len(item) == 2 for item in pairs], default= False)


def db_save(value):
    """
    saves a db_cell from the database. Will iterates through lists and dicts

    :Parameters:
    ------------
    value: obj
        db_cell (or list/dict of) to be loaded 
        
    :Example:
    ---------
    >>> from pyg import *
    >>> db = partial(mongo_table, table = 'test', db = 'test', pk = ['a','b'])
    >>> c = db_cell(add_, a = 2, b = 3, key = 'test', db = db)
    >>> c = db_save(c)    
    >>> assert get_cell('test', 'test', a = 2, b = 3).key == 'test'
        

    """
    if isinstance(value, db_cell):
        return value.save()
    elif isinstance(value, (tuple, list)):
        return type(value)([db_save(v) for v in value])
    elif isinstance(value, dict):
        return type(value)(**{k : db_save(v) for k, v in value.items()})
    else:
        return value

def db_load(value, mode = 0):
    """
    loads a db_cell from the database. Iterates through lists and dicts
    
    :Parameters:
    ------------
    value: obj
        db_cell (or list/dict of) to be loaded 
    
    mode: int
        loading mode -1: dont load, +1: load and throw an exception if not found, 0: load if found
    
    """
    if isinstance(value, db_cell):
        return value.load(mode)
    elif isinstance(value, (tuple, list)):
        return type(value)([db_load(v, mode = mode) for v in value])
    elif isinstance(value, dict):
        return type(value)(**{k : db_load(v, mode = mode) for k, v in value.items()})
    else:
        return value
    
def _load_asof(table, kwargs, deleted):    
    t = table.inc(kwargs)
    live = t.inc(-q.deleted)
    l = live.count()
    if deleted in (False, None): # we just want live values
        if l == 0:
            raise ValueError('no undeleted cells found matching %s'%kwargs)        
        elif l>1:
            raise ValueError('multiple cells found matching %s'%kwargs)
        return live[0]
    else:        
        past = t.inc(+q.deleted) if deleted is True else t.inc(+q.deleted, q.deleted > deleted) ## it is possible to have cells with deleted in self
        p = past.count()
        if p == 1:
            return past[0]
        elif p > 1:
            if deleted is True:
                raise ValueError('multiple historic cells are avaialble %s with these dates: %s. set delete = DATE to find the cell on that date'%(kwargs, past.deleted))
            else:
                return past.sort('deleted')[0]
        else:    ## no records found in past, we go to the deleted history
            history = t.deleted if deleted is True else t.deleted.inc(q.deleted > deleted) #cells deleted after deleted date
            h = history.count()
            if h == 0:
                if l > 0:
                    raise ValueError('no deleted cells found matching %s but a live one exists. Set deleted = False to get it'%kwargs)        
                else:                   
                    raise ValueError('no deleted cells found matching %s'%kwargs)        
            elif h>1:
                if deleted is True:
                    raise ValueError('multiple historic cells are avaialble %s with these dates: %s. set delete = DATE to find the cell on that date'%(kwargs, history.deleted))
                else:
                    return history.sort('deleted')[0]
            else:
                return history[0]


class db_cell(cell):
    """
    a db_cell is a specialized cell with a 'db' member pointing to a database where cell is to be stored.    
    We use this to implement save/load for the cell.
    
    It is important to recognize the duality in the design:
    - the job of the cell.db is to be able to save/load based on the primary keys.
    - the job of the cell is to provide the primary keys to the db object.
    
    The cell saves itself by 'presenting' itself to cell.db() and say... go on, load my data based on my keys. 
    
    :Example: saving & loading
    --------------------------
    >>> from pyg import *
    >>> people = partial(mongo_table, db = 'test', table = 'test', pk = ['name', 'surname'])
    >>> anna = db_cell(db = people, name = 'anna', surname = 'abramzon', age = 46).save()
    >>> bob  = db_cell(db = people, name = 'bob', surname = 'brown', age = 25).save()
    >>> james = db_cell(db = people, name = 'james', surname = 'johnson', age = 39).save()


    Now we can pull the data directly from the database

    >>> people()['name', 'surname', 'age'][::]
    >>> dictable[3 x 4]
    >>> _id                     |age|name |surname 
    >>> 601e732e0ef13bec9cd8a6cb|39 |james|johnson 
    >>> 601e73db0ef13bec9cd8a6d4|46 |anna |abramzon
    >>> 601e73db0ef13bec9cd8a6d7|25 |bob  |brown       

    db_cell can implement a function:

    >>> def is_young(age):
    >>>    return age < 30
    >>> bob.function = is_young
    >>> bob = bob.go()
    >>> assert bob.data is True

    When run, it saves its new data to Mongo and we can load its own data:

    >>> new_cell_with_just_db_and_keys = db_cell(db = people, name = 'bob', surname = 'brown')
    >>> assert 'age' not in new_cell_with_just_db_and_keys 
    >>> now_with_the_data_from_database = new_cell_with_just_db_and_keys.load()
    >>> assert now_with_the_data_from_database.age == 25

    >>> people()['name', 'surname', 'age', 'data'][::]
    >>>  dictable[3 x 4]
    >>> _id                     |age|name |surname |data
    >>> 601e732e0ef13bec9cd8a6cb|39 |james|johnson |None
    >>> 601e73db0ef13bec9cd8a6d4|46 |anna |abramzon|None
    >>> 601e73db0ef13bec9cd8a6d7|25 |bob  |brown   |True
    >>> people().reset.drop()    

    """

    def __init__(self, function = None, output = None, db = None, **kwargs):
        if db is not None:
            if not isinstance(db, partial):
                raise ValueError('db must be a partial of a function like mongo_table initializing a mongo cursor')
            super(db_cell, self).__init__(function = function, output = output, db = db, **kwargs)
        else:
            self[_db] = None
            super(db_cell, self).__init__(function = function, output = output, **kwargs)

    @property
    def _pk(self):
        if self.get(_db) is None:
            return super(db_cell, self)._pk
        else:
            return self.db.keywords.get(_pk)        

    @property
    def _address(self):
        """
        :Example:
        ----------
        >>> from pyg import *
        >>> self = db_cell(db = partial(mongo_table, 'test', 'test', pk = 'key'), key = 1)
        >>> db = self.db()
        >>> self._address
        >>> self._reference()
        >>> self.get('key')
        
        :Returns:
        -------
        tuple
            returns a tuple representing the unique address of the cell.
        """
        if self.get(_db) is None:
            return super(db_cell, self)._address
        db = self.db()
        return db.address + tuple([(key, self.get(key)) for key in db._pk])


    def _clear(self):
        """
        Removes most of the data from the cell. Just keeps it so that we have enough data to load it back from the database

        :Returns:
        -------
        db_cell
            skeletal reference to the database

        """
        if self.get(_db) is None: 
            return super(db_cell, self)._clear()
        else:
            return self[[_db, _function, _updated] + self.db().pk] if _updated in self else self[[_db, _function] + self.db().pk]

    def save(self):
        if self.get(_db) is None:
            return super(db_cell, self).save()
        address = self._address
        doc = (self - _deleted)
        db = self.db()
        missing = ulist(db._pk) - self.keys()
        if len(missing):
            logger.warning('WARN: document not saved as some keys are missing %s'%missing)
            return self            
        ref = type(doc)(**cell_clear(dict(doc)))
        try:
            updated = db.update_one(ref)
        except Exception:
            updated = db.update_one(ref - db._ids)
        for i in db._ids:
            doc[i] = updated[i]
        get_GRAPH()[address] = doc
        return doc
                
        
    def load(self, mode = 0):
        """
        loads a document from the database and updates various keys.
        
        :Persistency:
        -------------
        Since we want to avoid hitting the database, there is a singleton GRAPH, a dict, storing the cells by their address.
        Every time we load/save from/to Mongo, we also update GRAPH.
        
        We use the GRAPH often so if you want to FORCE the cell to go to the database when loading, use this:

        >>> cell.load(-1) 
        >>> cell.load(-1).load(0)  # clear GRAPH and load from db
        >>> cell.load([0])     # same thing: clear GRAPH and then load if available

        :Merge of cached cell and calling cell:
        ----------------------------------------
        Once we load from memory (either MongoDB or GRAPH), we tree_update the cached cell with the new values in the current cell.
        This means that it is next to impossible to actually *delete* keys. If you want to delete keys in a cell/cells in the database, you need to:
        
        >>> del db.inc(filters)['key.subkey']

        :Parameters:
        ----------
        mode : int , dataetime, optional
            if -1, then does not load and clears the GRAPH
            if 0, then will load from database if found. If not found, will return original document
            if 1, then will throw an exception if no document is found in the database
            if mode is a date, will return the version alive at that date 
            The default is 0.
            
            IF you enclose any of these in a list, then GRAPH is cleared prior to running and the database is called.
    
        :Returns:
        -------
        document

        """
        if self.get(_db) is None:
            return super(db_cell, self).load(mode = mode)
        if isinstance(mode, (list, tuple)):
            if len(mode) == 0:
                return self.load()
            if len(mode) == 1:
                return self.load(mode[0])
            if len(mode) == 2 and mode[0] == -1:
                res = self.load(-1)
                return res.load(mode[-1])
            else:
                raise ValueError('mode can only be of the form [], [mode] or [-1, mode]')
        db = self.db(mode = 'w')
        pk = ulist(db._pk)
        missing = pk - self.keys()
        if len(missing):
            logger.warning('WARN: document not loaded as some keys are missing %s'%missing)
            return self            
        address = self._address
        kwargs = {k : self[k] for k in pk}
        graph = get_GRAPH()
        if mode == -1:
            if address in graph:
                del graph[address]
            return self
        if address not in graph: # we load from the database
            if is_date(mode):
                graph[address] = _load_asof(db, kwargs, deleted = mode)
            else:
                try:
                    graph[address] = _load_asof(db, kwargs, deleted = False)
                except Exception:
                    if mode in (1, True):
                        raise ValueError('no cells found matching %s'%kwargs)
                    else:
                        return self         
        if address in graph:
            saved = graph[address] 
            self_updated = self.get(_updated)
            saved_updated = saved.get(_updated)
            if self_updated is None or (saved_updated is not None and saved_updated > self_updated):
                excluded_keys = (self /  None).keys() - self._output - _updated
            else:
                excluded_keys = (self /  None).keys()
            if is_date(mode):
                excluded_keys += [_id]
            update = (saved / None) - excluded_keys
            self.update(update)
        return self        

    def push(self):        
        me = self._address
        res = self.go() # run me on my own as I am not part of the push
        cell_push(me, exc = 0)
        UPDATED = get_cache('UPDATED')
        if me in UPDATED:
            del UPDATED[me]
        return res

    def bind(self, **bind):
        """
        bind adds key-words to the primary keys of a cell

        :Parameters:
        ----------
        bind : dict
            primary keys and their values.
            The value can be a callable function, transforming existing values

        :Returns:
        -------
        res : cell
            a cell with extra binding as primary keys.

        :Example:
        ---------
        >>> from pyg import *
        >>> db = partial(mongo_table, 'test', 'test', pk = 'key')
        >>> c = db_cell(passthru, data = 1, db = db, key = 'old_key')()
        >>> d = c.bind(key = 'key').go()
        >>> assert d.pk == ['key']
        >>> assert d._address in GRAPH
        >>> e = d.bind(key2 = lambda key: key + '1')()
        >>> assert e.pk == ['key', 'key2']
        >>> assert e._address == (('key', 'key'), ('key2', 'key1'))
        >>> assert e._address in GRAPH
        """
        db = self.get(_db)
        if db is None:
            return super(db_cell, self).bind(**bind)
        else:
            kw = self.db.keywords
            for k in bind: # we want to be able to override tables/db/url
                if k in ['db', 'table', 'url']:
                    kw[k] = bind.pop(k)
            pk = sorted(set(as_list(kw.get(_pk))) | set(bind.keys()))
            kw[_pk] = pk
            db = partial(db.func, *db.args, **kw)
            res = Dict({key: self.get(key) for key in pk})
            res = res(**bind)
            res[_db] = db
            return self + res


def cell_push(nodes = None, exc = None):
    UPDATED = get_cache('UPDATED')
    GRAPH = get_cache('GRAPH')
    if nodes is None:
        nodes = UPDATED.keys()
    children = [child for child in descendants(get_DAG(), nodes, exc = exc) if child is not None]
    for child in children:
        GRAPH[child] = (GRAPH[child] if child in GRAPH else get_cell(**dict(child))).go()
    for child in children:
        del UPDATED[child]




def cell_pull(nodes, types = cell):
    for node in as_list(nodes):
        node = node.pull()
        _GAD = get_GAD()
        children = [get_cell(**dict(a)) for a in _GAD.get(node._address,[])]        
        cell_pull(children, types)
    return None        



def _get_cell(table = None, db = None, url = None, deleted = None, _from_memory = None, **kwargs):
    """
    retrieves a cell from a table in a database based on its key words. In addition, can look at earlier versions using deleted.
    It is important to note that this DOES NOT go through the cache mechanism but goes to the database directly every time.

    :Parameters:
    ----------
    table : str
        name of table (Mongo collection). alternatively, you can just provide an address
    db : str
        name of database.
    url : TYPE, optional
        DESCRIPTION. The default is None.
    deleted : datetime/None, optional
        The date corresponding to the version of the cell we want
        None = live cell
        otherwise, the cell that was first deleted after this date.
    **kwargs : keywords
        key words for grabbing the cell.

    :Returns:
    -------
    The document

    :Example:
    ---------
    >>> from pyg import *
    >>> people = partial(mongo_table, db = 'test', table = 'test', pk = ['name', 'surname'])
    >>> brown = db_cell(db = people, name = 'bob', surname = 'brown', age = 39).save()
    >>> assert get_cell('test','test', surname = 'brown').name == 'bob'
        
    """
    GRAPH = get_cache('GRAPH')
    if is_pairs(table):
        params = dict(table)
        params.update({key: value for key, value in dict(db = db, 
                                                         url = url, 
                                                         deleted = deleted,
                                                         _from_memory = _from_memory
                                                         ).items() if value is not None})
        params.update(kwargs)
        return _get_cell(**params)
    
    pk = kwargs.pop('pk', None)
    if pk is None:
        address = kwargs_address = tuple(sorted(kwargs.items()))
    else:
        pk = sorted(as_list(pk))
        address = kwargs_address = tuple([(key, kwargs.get(key)) for key in pk]) 
   
    if db is not None and table is not None:
        t = mongo_table(db = db, table = table, url = url, pk = pk)
        address = t.address + kwargs_address
        if _from_memory and deleted in (None, False): # we want the standard cell
            if address not in GRAPH:
                GRAPH[address] = _load_asof(t, kwargs, deleted)
            return GRAPH[address]
        else:
            return _load_asof(t, kwargs, deleted) # must not overwrite live version. User wants a specific deleted version
    else:
        return GRAPH[address]


def load_cell(table = None, db = None, url = None, deleted = None, **kwargs):
    """
    retrieves a cell from a table in a database based on its key words. 
    In addition, can look at earlier versions using deleted.
    It is important to note that this DOES NOT go through the cache mechanism 
    but goes to the database directly every time.

    :Parameters:
    ----------
    table : str
        name of table (Mongo collection). alternatively, you can just provide an address
    db : str
        name of database.
    url : TYPE, optional
        DESCRIPTION. The default is None.
    deleted : datetime/None, optional
        The date corresponding to the version of the cell we want
        None = live cell
        otherwise, the cell that was first deleted after this date.
    **kwargs : keywords
        key words for grabbing the cell.

    :Returns:
    ---------
    The document

    :Example:
    ---------
    >>> from pyg import *
    >>> people = partial(mongo_table, db = 'test', table = 'test', pk = ['name', 'surname'])
    >>> brown = db_cell(db = people, name = 'bob', surname = 'brown', age = 39).save()
    >>> assert load_cell('test','test', surname = 'brown').name == 'bob'
        
    """
    return _get_cell(table = table, db = db, url = url, deleted = deleted, **kwargs)

def get_docs(table = None, db = None, url = None, pk = None, cell = 'data', **kwargs):
    """
    retrieve multiple cells from a table

    """
    t = mongo_table(db = db, table = table, url = url, pk = pk).inc(**kwargs)    
    return t.docs(list(kwargs.keys()))
    
    
def get_cell(table = None, db = None, url = None, deleted = None, **kwargs):
    """
    unlike load_cell which will get the data from the database by default 
    get cell looks at the in-memory cache to see if the cell exists there.

    :Parameters:
    ----------
    table : str
        name of table (Mongo collection). alternatively, you can just provide an address
    db : str
        name of database.
    url : TYPE, optional
        DESCRIPTION. The default is None.
    deleted : datetime/None, optional
        The date corresponding to the version of the cell we want
        None = live cell
        otherwise, the cell that was first deleted after this date.
    **kwargs : keywords
        key words for grabbing the cell.

    :Returns:
    -------
    The document

    :Example:
    ---------
    >>> from pyg import *
    >>> people = partial(mongo_table, db = 'test', table = 'test', pk = ['name', 'surname'])
    >>> brown = db_cell(db = people, name = 'bob', surname = 'brown', age = 39).save()
    >>> assert get_cell('test','test', surname = 'brown').name == 'bob'
    """
    _from_memory = kwargs.pop('_from_memory', True)
    return _get_cell(table = table, db = db, url = url, deleted = deleted, _from_memory = _from_memory, **kwargs)


def get_data(table = None, db = None, url = None, deleted = None, **kwargs):
    """
    retrieves a cell from a table in a database based on its key words. 
    In addition, can look at earlier versions using deleted.

    :Parameters:
    ----------
    table : str
        name of table (Mongo collection).
    db : str
        name of database.
    url : TYPE, optional
        DESCRIPTION. The default is None.
    deleted : datetime/None, optional
        The date corresponding to the version of the cell we want
        None = live cell
        otherwise, the cell that was first deleted after this date.
    **kwargs : keywords
        key words for grabbing the cell.

    :Returns:
    -------
    The document

    :Example:
    ---------
    >>> from pyg import *
    >>> people = partial(mongo_table, db = 'test', table = 'test', pk = ['name', 'surname'])
    >>> people().reset.drop()
    >>> brown = db_cell(db = people, name = 'bob', surname = 'brown', age = 39).save()
    >>> assert get_data('test','test', surname = 'brown') is None
        
    """    
    return cell_item(get_cell(table, db = db, url = url, deleted = deleted, **kwargs), key = 'data')

def load_data(table = None, db = None, url = None, deleted = None, **kwargs):
    """
    retrieves a cell from a table in a database based on its key words. 
    In addition, can look at earlier versions using deleted.

    :Parameters:
    ----------
    table : str
        name of table (Mongo collection).
    db : str
        name of database.
    url : TYPE, optional
        DESCRIPTION. The default is None.
    deleted : datetime/None, optional
        The date corresponding to the version of the cell we want
        None = live cell
        otherwise, the cell that was first deleted after this date.
    **kwargs : keywords
        key words for grabbing the cell.

    :Returns:
    -------
    The document

    :Example:
    ---------
    >>> from pyg import *
    >>> people = partial(mongo_table, db = 'test', table = 'test', pk = ['name', 'surname'])
    >>> people().reset.drop()
    >>> brown = db_cell(db = people, name = 'bob', surname = 'brown', age = 39).save()
    >>> assert load_data('test','test', surname = 'brown') is None
        
    """    
    return cell_item(load_cell(table, db = db, url = url, deleted = deleted, **kwargs), key = 'data')

