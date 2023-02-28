from pyg_base import is_str, cfg_read, get_cache, cache
from pyg_mongo._reader import mongo_reader
from pyg_mongo._cursor import mongo_cursor, mongo_pk_cursor


from pymongo import MongoClient

__all__ = ['mongo_table']


def _url(url):
    """
    converts the URL address to actual url based on the cfg['mongo'] locations. see cfg_read() for help.
    """
    cfg = cfg_read()
    mongo = cfg.get('mongo', {})
    _url = url or 'null'
    if _url in mongo:
        return mongo[_url]
    else:
        return url
    

_mongo_table_cache = get_cache('mongo_table') 
_mongo_table_cache['w'] = mongo_cursor, mongo_pk_cursor, MongoClient
_mongo_table_cache['r'] = mongo_reader, mongo_reader, MongoClient


@cache
def _mongo_table(table, db, pk = None, url = None, reader = None, writer = None, mode = 'w', **kwargs):    
    if mode is None:
        mode = 'w'
    if is_str(mode):
        mode = mode.lower()
        if_no_pk, if_pk, client = _mongo_table_cache[mode]
        obj = if_no_pk if pk is None else if_pk
    else:
        obj = mode
        client = MongoClient
    url = _url(url)
    c = client(url)[db][table]
    res = obj(c, pk = pk, writer = writer, reader = reader, **kwargs)
    if isinstance(res, (mongo_reader)) and len(res) == 0:
         res.create_index()
    return res


def mongo_table(table, db, pk = None, url = None, reader = None, writer = None, mode = 'w', **kwargs):    
    """
    mongo table is the entry point for multiple mongo_cursor objects.
    
    :Parameters:
    ------------
    table: str
        name of mongo table
    
    db: str
        name of mongo database
    
    pk: None/str/list of string
        primary keys associated with the table. if left blank, this will behave like a usual mongo table with duplicate documents possible.
        If primary keys are provided, documents will be managed like sql tables: we maintain a table with unique primary keys and e.g. we are unable to insert a document without primary keys
        
    url: str
        url for mongodb connection, defaults to localhost
        
    reader: callable(s)
        function(s) applied to the document read from MongoDB before returning to user. If left blank, will default to pyg_encoders.decode
    
    
    writer: str/callables
        function to be applied to the document before writing it into the database. If left blank, will default to pyg_encoders.encode.
        writer can be a generic string that allows me to save objects within the document into files.
        For example:
            >>> writer = 'c:/%name/%surname.parquet'
        This will take pandas Series/DataFrame and save them as parquet file. %name and %surname will be populated based on the name and surname key inside the document
            >>> doc = dict(name = 'abraham', surname = 'lincoln', data = pd.Series([1,2]), other = dict(key = [pd.DataFrame([1,2]), pd.DataFrame([2,3])]))
        
        The writer will save 
            data key in c:/abraham/lincoln/data.parquet
            the list in doc[other][key] in c:/abraham/lincoln/other/key/0.parquet and c:/abraham/lincoln/other/key/1.parquet
        
    mode: str 
        'w' or 'r'. defaults to writer. if 'r', all writing functions are disabled
    
    :Example: simple mongo table
    ---------
    >>> table = mongo_table('table', 'db')
    >>> table.drop()
    
    <class 'pyg_mongo._cursor.mongo_cursor'> for Collection(Database(MongoClient(host=['localhost:27017'], document_class=dict, tz_aware=False, connect=True), 'db'), 'table') 
    M{} None
    documents count: 0 
        
    >>> table.insert_one(dict(a = 1 , b = 2))
    >>> table.insert_one(dict(a = 1 , b = 3))
    >>> table.insert_one(dict(a = 2 , b = 3))
    >>> assert len(table) == 3
    
    >>> assert len(table.find(a = 1)) == 2
    >>> assert len(table.inc(a = 1).exc(b = 3)) == 1
        
    Example: simple access
    -------
    >>> table[0]
    {'_id': ObjectId('63fce51c57002832d3dc3b66'), 'a': 1, 'b': 2}

    >>> table.insert_one(dict(a = 2 , b = 0))
    >>> assert table.sort('b')[0]['b'] == 0 ### table is sorted on 'b' key
    
    Example: simple access using distinct
    -------
    >>> assert table.b == [0,2,3] ## distinct b
 
    Example: simple access using iteration
    -------
    >>> assert [doc['a'] for doc in table.sort('a')] == [1,1,2,2]

     
    :Example: handling MotorClient vs MongoClient for asynchronous database writing
    ---------
    from pyg import *
    from pymongo import MongoClient
    from motor import MotorClient

    table = db = 'test'
    pk = 'key'
    url = reader = writer = None    
    mode = 'aw'    
    kwargs = {}
    client = MotorClient(url)
    c = client[db][table]

    isinstance(cursor, ())
    res = obj(c, pk = pk, writer = writer, reader = reader, **kwargs)
    
    await c.insert_one(dict(a = 1))
    await c.create_index(dict(a = 1))

    cfg = cfg_read()
    
    """ 
    return _mongo_table(table, db, pk = pk, url = url, reader = reader, writer = writer, mode = mode, **kwargs)      
