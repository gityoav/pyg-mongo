from pyg_base import is_str, cfg_read, get_cache
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


def mongo_table(table, db, pk = None, url = None, reader = None, writer = None, mode = 'w', **kwargs):    
    """
    :Example:
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

        
