from pyg_base import passthru, decode, is_str, as_list
import pymongo as pym
from pyg_mongo._encoders import csv_write, parquet_write, _csv, _parquet, encode
from functools import partial

__all__ = ['is_collection', 'is_cursor', 'as_collection']

# def is_client(value):
#     """is the value a pymongo.MongoClient"""
#     return isinstance(value, pym.mongo_client.MongoClient)

# def is_db(value):
#     """is the value a pymongo.Database"""
#     return isinstance(value, pym.database.Database)

def is_collection(value):
    """ is the value a pymongo.Collection (equivalent of a table)"""
    return isinstance(value, (pym.collection.Collection))

def is_cursor(value):
    """ is the value a pymongo.Cursor"""
    return isinstance(value, (pym.cursor.Cursor))


def as_collection(collection):
    if is_collection(collection):
        return collection
    elif is_cursor(collection):
        return collection.collection
    

