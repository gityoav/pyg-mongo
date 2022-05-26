from pyg_base._cell import _pk
from pyg_base import is_dict, as_list, ulist, cache, dt, is_strs, Dict, sort
from pyg_base import passthru, decode, is_str
from pyg_mongo._q import q, _id
from pyg_mongo._encoders import csv_write, parquet_write, npy_write, _csv, _npy, _npa, _parquet, encode
from functools import partial

_root = 'root'

def as_reader(reader):
    if isinstance(reader, list):
        return sum([as_reader(r) for r in reader], [])
    elif reader is None or reader is True or reader == ():
        return [decode]
    elif reader is False or reader == 0:
        return [passthru]
    else:
        return [reader]


_writers = {_csv: csv_write , _npy: partial(npy_write, append = False), _npa: partial(npy_write, append = True), _parquet: parquet_write}

def as_writer(writer):
    if isinstance(writer, list):
        return sum([as_writer(w) for w in writer], [])
    if writer is None or writer is True or writer == ():
        return [encode]
    elif writer is False or writer == 0:
        return [passthru]
    elif is_str(writer):
        for ext, w in _writers.items():
            if writer.endswith(ext):
                root = writer[:-len(ext)]
                if root:
                    return [partial(w, root = root), encode]
                else:
                    return [w, encode]
        raise ValueError('We support only csv/npy/parquet writers and writer should look like: c:/somewhere/%name/%surname.csv or d:/archive/%country/%city/results.parquet or with .npy')
    else:
        return as_list(writer)

def _dict1(keys):
    if keys is None or is_dict(keys):
        return keys
    else:
        return dict([(key[1:], -1) if key.startswith('-') else (key,1) for key in as_list(keys)])


def _items1(keys):
    return list(_dict1(keys).items()) if keys else []


@cache
def _pkq(pk):
    """
    Returns a query based on pk.
    Note that these are designed to integrate with cells and how they are saved in the database
    
    
    :Parameters:
    ----------------
    pk : list 
        list of primary keys
        
    :Returns:
    -------
    dict 
        mongo query filtering for table

    :Examples:
    ----------
    >>> import datetime
    >>> assert _pkq(None) == {}
    >>> assert dict(_pkq(['world', 'hello'])) == {"_pk": {"$eq": ["hello", "world"]}}

    """
    if pk is None or len(pk) == 0:
        return {}
    else:
        return q[_pk] == [pk]

_empty_crsr = Dict(collection = None, spec = None, projection = None, sorter = None, reader = None, writer = None, pk = None)
_attrs = ['collection', 'projection', 'sorter', 'reader', 'writer', 'pk']

class mongo_base_reader(object):
    """
    The base reader handles functionality shared between async and standard readers.
    It handles the calculation of cursor spec, projection and sorting.
    
    
    The object layout is as follows:
            
                                base_reader
                        
            mongo_reader                            mongo_async_reader           
            
    mongo_cursor     mongo_pk_cursor       mongo_async_cursor     mongo_async_pk_cursor
                     
                
    """
    def __init__(self, collection, spec = None, projection = None, sorter = None, reader = None, writer = None, pk = None):
        if isinstance(collection, mongo_base_reader):
            crsr = collection
            collection = crsr.collection
        else: 
            crsr = _empty_crsr 
        self.collection = collection 
        self.spec = crsr.spec if spec is None else spec
        self.projection = crsr.projection if projection is None else projection
        self.sorter  = crsr.sorter  if sorter   is None else sorter
        self.reader  = crsr.reader  if reader   is None else reader
        self.writer  = crsr.writer  if writer   is None else writer 
        self.pk      = crsr.pk      if pk       is None else pk
        self.pk = self._pk

    def _callargs(self, **kwargs):
        spec = kwargs.pop('spec', None)
        if spec is False:
            spec = None
        else:
            spec = self.spec if spec is None else q(self.spec, spec)            
        keywords = {attr: kwargs.get(attr, getattr(self, attr)) for attr in _attrs}
        keywords['spec'] = spec            
        return keywords

    def __call__(self, **kwargs):
        callargs = self._callargs(**kwargs)
        return type(self)(**callargs)

    def find(self, *args, **kwargs):
        res = self.copy()
        res.spec = q(self.spec, *args, **kwargs)
        return res
    
    inc = find
    
    def sort(self, *sorter):
        return self(sorter = as_list(sorter))

    def project(self, projection = None):
        return self(projection = projection)

    
    def exc(self, **kwargs):
        """
        filters 'negatively' removing documents that match the criteria specified. 

        Returns
        -------
        cursor
            filtered documents.
        
        :Example:
        ---------
        >>> from pyg import *; import pymongo
        >>> table = pymongo.MotorClient()['test']['test']
        >>> table.insert_one(dict(name = 'alan', surname = 'abrahams', age = 39, marriage = dt(2000)))
        >>> table.insert_one(dict(name = 'barbara', surname = 'brown', age = 50, marriage = dt(2020)))
        >>> table.insert_one(dict(name = 'charlie', surname = 'cohen', age = 20))

        >>> t = mongo_reader(table)
        >>> assert len(t.exc(name = 'alan')) == 2        
        >>> assert len(t.exc(name = ['alan', 'barbara'])) == 1        
        >>> table.drop()
        """
        query = [getattr(q, key)!=value for key, value in kwargs.items()]
        return self.inc(*query)


    @property
    def _spec(self):
        return q(self.spec, _pkq(self._pk))

    @property
    def _projection(self):
        return _dict1(self.projection)

    @property
    def _sort(self):
        return list(_dict1(self.sorter).items()) if self.sorter else None
    
    @property
    def _pk(self):
        return ulist(sorted(set(as_list(self.pk))))

    def distinct(self, key):
        """
        returns the distinct cursor values of the key        
        """
        res = self.cursor.distinct(key)
        try:
            return sort(res)
        except TypeError:
            return res

    def _is_deleted(self):
        return self.collection.database.name.startswith('deleted_')

    @property
    def deleted(self):
        if self._is_deleted():
            return self.distinct('deleted')
        collection_name = self.collection.name
        db = self.collection.database
        db_name = db.name
        collection = db.client['deleted_' + db_name][collection_name]
        return type(self)(collection, spec = self.spec, projection = self.projection, sorter = self.sorter, reader = self.reader, writer = self.writer, pk = self.pk) 

    @property
    def cursor(self):
        res = self.collection.find(self._spec, self._projection)
        sorter = self.sorter
        if sorter:
            res = res.sort(self._sort)
        return res

    @property
    def address(self):
        """
        :Returns:
        ---------
        tuple
            A unique combination of the client addres, database name and collection name, identifying the collection uniquely.

        """
        return ('url', '%s:%s' % self.collection.database.client.address), ('db', self.collection.database.name), ('table', self.collection.name)


    def _reader(self, reader = None):
        return as_reader(self.reader if reader is None else reader)

    def _read(self, doc, reader = None):
        """
        converts doc from Mongo into something we want
        """
        res = doc
        reader = self._reader(reader)
        for r in as_list(reader):
            res = res[r] if is_strs(r) else r(res)
        return res
        
    def _writer(self, writer = None, doc = None):
        doc = doc or {}
        if writer is None:
            writer = doc.get(_root)
        if writer is None:
            writer = self.writer
        return as_writer(writer)
            

    def _write(self, doc, writer = None):
        res = doc.copy()
        writer = self._writer(writer, doc)
        for w in writer:
            res = w(res)
        pk = self._pk
        missing = set(pk) - set(doc.keys())
        if len(missing):
            raise ValueError('trying to write a document with missing primary keys %s'%missing)
        if pk:
            res.update({_pk: pk})
        return res

    def _id(self, doc):
        if _id in doc:
            return q[_id] == decode(doc[_id])
        elif self.pk:
            pk = self._pk
            return q(q[_pk] == [pk], **{key : doc[key] for key in pk if key in doc})
        else:
            return doc


    def __repr__(self):
        return '%(t)s for %(c)s \nfilter: %(s)s projection: %(p)s sorted: %(r)s'%dict(t = type(self), 
                                                                                      c = self.collection, 
                                                                                      s = self._spec, 
                                                                                      p = self._projection,
                                                                                      r = self._sort)    

    def copy(self):
        return type(self)(self)

    clone = copy
    
    def __eq__(self, other):
        return type(other) == type(self) and self.reader == other.reader and self.writer == other.writer \
            and self.collection == other.collection and self.spec == other.spec and self.projection == other.projection \
            and self.sorter == other.sorter

    @property
    def reset(self):
        return type(self)(self.collection, writer = self.writer, reader = self.reader)

    def insert_one(self, *_, **__):
        raise AttributeError('reader is read-only')
    def insert_many(self, *_, **__):
        raise AttributeError('reader is read-only')
    def delete_one(self, *_, **__):
        raise AttributeError('reader is read-only')
    def delete_many(self, *_, **__):
        raise AttributeError('reader is read-only')
    def set(self, *_, **__):
        raise AttributeError('reader is read-only')
    def drop(self, *_, **__):
        raise AttributeError('reader is read-only')

