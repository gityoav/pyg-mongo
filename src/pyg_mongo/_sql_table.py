import sqlalchemy as sa
from sqlalchemy_utils.functions import create_database
from pyg_base import cache, cfg_read, named_dict, dumps, as_list, dictattr, dictable, Dict, decode, is_dict, is_dictable, is_strs, is_str, is_int, ulist, encode, passthru
from pyg_mongo._writers import as_reader, as_writer
from sqlalchemy import Table, Column, Integer, String, MetaData, Identity, Float, DATE, DATETIME, TIME, select, func, not_, and_, or_, desc, asc
from sqlalchemy.orm import Session
import datetime
from copy import copy
import json

_id = '_id'
_doc = 'doc'
_root = 'root'

DRIVER = None
SERVER = None

def _server(server = None):
    if server is None or server is True:
        server = SERVER
    if server is None or server is True:
        server = cfg_read().get('sql_server')
    if server is None:
        raise ValueError('please provide server or set a "sql_server" in cfg file: from pyg_base import *; cfg = cfg_read(); cfg["sql_server"] = "server"; cfg_write(cfg)')
    return server

def _driver(driver = None):
    driver = driver or DRIVER
    if driver is None or driver is True:
        driver = cfg_read().get('sql_driver')
    if driver is None:
        import pyodbc
        odbc_drivers = [d for d in pyodbc.drivers() if d.startswith('ODBC')]
        if len(odbc_drivers):
            driver = sorted(odbc_drivers)[-1].replace(' ', '+')
        if driver is None:
            raise ValueError('No ODBC drivers found for SQL Server, please save one: cfg = cfg_read(); cfg["sql_driver"] = "ODBC+Driver+17+for+SQL+Server"; cfg_write(cfg)')    
        else:
            return driver
    elif is_int(driver):
        return 'ODBC+Driver+%i+for+SQL+Server'%driver
    else:
        return driver
    
DRIVER = _driver()
SERVER = _server()
    
def get_cstr(db = 'master', server = None, driver = None, trusted_connection = 'yes', user = None, password = None):
    server = _server(server) 
    driver = _driver(driver)
    if '//' in server:
        return server
    else:
        connection = {k:v for k,v in dict(driver = driver, trusted_connection = trusted_connection, user = user, password = password).items() if v is not None}
        params = '&'.join('%s=%s'%(k,v) for k,v in connection.items())        
        return 'mssql+pyodbc://%(server)s/%(db)s%(params)s'%dict(server=server, db = db or 'master', params = '?' +params if params else '')

def get_engine(db = 'master', server = None, driver = None, trusted_connection = 'yes', user = None, password = None):    
    if isinstance(server, sa.engine.base.Engine):
        return server
    driver = _driver()
    cstr = get_cstr(server=server, db = db, driver = driver, trusted_connection = trusted_connection , user = user, password = password)    
    e = sa.create_engine(cstr)
    try:
        sa.inspect(e)
    except Exception:
        print('creating db... ', db)
        create_database(cstr)
        e = sa.create_engine(cstr)       
    return e
    
_types = {str: String, int : Integer, float: Float, datetime.date: DATE, datetime.datetime : DATETIME, datetime.time: TIME}
_orders = {1 : asc, True: asc, 'asc': asc, asc : asc, -1: desc, False: desc, 'desc': desc, desc: desc}


def get_sql_table(table, db = None, non_null = None, nullable = None, _id = None, schema = None, server = None, reader = None, writer = None, pk = None, doc = None, mode = None):
    """
    Creates a basic sql table. Can also be used to simply read table from the db
    """
    if isinstance(table, str):
        values = table.split('.')
        if len(values) == 2:
            db = db or values[0]
            if db != values[0]:
                raise ValueError('db cannot be both %s and %s'%(values[0], db))
            table = values[1]
        elif len(values)>2:
            raise ValueError('not sure how to translate this %s into a db.table format'%table)
    e = get_engine(server = server, db = db)
    non_null = non_null or {}
    nullable = nullable or {}
    if isinstance(non_null, list):
        non_null = {k : String for k in non_null}
    if isinstance(nullable, list):
        nullable = {k : String for k in nullable}
    if isinstance(table, str):
        table_name = table 
    else:
        table_name = table.name
        schema = schema or table.schema
    if doc is True:
        doc = _doc        
    meta = MetaData()
    i = sa.inspect(e)
    if not i.has_table(table_name, schema = schema):
        cols = []
        if isinstance(table, sa.sql.schema.Table):
            for col in table.columns:
                col = copy(col)
                del col.table
                cols.append(col)
        if _id is not None:
            if isinstance(_id, str):
                _id = {_id : int}
            if isinstance(_id, list):
                _id = {i : int for i in _id}
            for i, t in _id.items():
                if i not in [col.name for col in cols]:
                    if t == int:                    
                        cols.append(Column(i, Integer, Identity(always = True)))
                    elif t == datetime.datetime:
                        cols.append(Column(i, DATETIME(timezone=True), nullable = False, server_default=func.now()))
                    else:
                        raise ValueError('not sure how to create an automatic item with column %s'%t)

        col_names = [col.name for col in cols]
        non_nulls = [Column(k, _types.get(t, t), nullable = False) for k, t in non_null.items() if k not in col_names]
        nullables = [Column(k.lower(), _types.get(t, t)) for k, t in nullable.items() if k not in col_names] 
        docs = [Column(doc, String, nullable = True)] if doc is not None else []            
        cols = cols + non_nulls + nullables + docs
        tbl = Table(table_name, meta, *cols)
        meta.create_all(e)
    else:
        meta.reflect(e)
        tbl = meta.tables[table_name]
        cols = tbl.columns
        non_nulls = [Column(k, _types.get(t, t), nullable = False) for k, t in non_null.items()]
        if non_nulls is not None:
            for key in non_nulls:
                if key.name not in cols.keys():
                    raise ValueError('column %s does not exist in %s.%s'%(key, db, table_name))
                elif cols[key.name].nullable is True:
                    raise ValueError('WARNING: You defined %s as a primary but it is nullable in %s.%s'%(key, db, table_name))
    res = sql_table(table = tbl, db = db, server = server, engine = e, spec = None, selection = None, reader = reader, writer = writer, pk = pk, doc = doc)
    return res

def _tbl_insert_one(tbl, doc):
    res = {}
    for col in list(tbl.columns):
        res[col.name] = doc.get(col.name, None)
    res[_doc] = dumps(doc)
    tbl.insert(**res)
    return res


class sql_table(object):
    """
    sql_table is a thin wrapper of sqlalchemy (sa.Table), adding and simplifying functionaility:
    sa.Table holds the logic while the engine manages the connection. So the logic is usually:
        - create a statement
        - establish a connection
        - execute statement

            
    - sql_table holds both the Table and the engine objects so we can merge these operations into one call.
    - In addition, we maintain the 'statement' so that a final selction statement can be built gradually rather than all at once
    
    
    :Example:
    ---------
    >>> from pyg import * 
    >>> t = get_sql_table(db = 'test', table = 'students', non_null = ['name', 'surname'], 
                          _id = dict(_id = int, created = datetime.datetime), 
                          nullable =  dict(doc = str, details = str, dob = datetime.date, age = int, grade = float))
    >>> t = t.delete()
    >>> assert len(t) == 0
    
    >>> t = t.insert(name = 'yoav', surname = 'git')
    >>> t = t.insert(name = 'anna', surname = 'git')
    >>> assert len(t) == 2
    >>> t = t.insert(name = ['ayala', 'itamar', 'opher'], surname = 'gate')
    >>> t.inc(name = 'ayala').update(age = 17)
    
    """
    def __init__(self, table, db = None, engine = None, server = None, spec = None, selection = None, order = None, reader = None, writer = None, pk = None, doc = None, **_):
        """
        Parameters
        ----------
        table : sa.Table
            Our table
        db : string, optional
            Name of the db where table is.
        engine : sa,Engine, optional
            The sqlalchemy engine
        server : str , optional
            The server for the engine. If none, uses the default in pyg config file
        spec : sa.Expression, optional
            The "where" statement
        selection : str/list of str, optional
            The columns in "select"
        order : dict or list, optional
            The columns in ORDER BY. The default is None.

        Returns
        -------
        None.

        """
        if is_str(table):
            table = get_sql_table(table = table, db = db, server = server)
            
        if isinstance(table, sql_table):
            db = table.db if db is None else db
            engine = table.engine if engine is None else engine
            server = table.server if server is None else server
            spec = table.spec if spec is None else spec
            selection = table.selection if selection is None else selection
            order = table.order if order is None else order
            reader = table.reader if reader is None else reader
            writer = table.writer if writer is None else writer
            pk = table.pk if pk is None else pk
            doc = table.doc if doc is None else doc
            table = table.table
    
        self.table = table
        self.db = db
        self.server = _server(server)
        self.engine = engine or get_engine(db = self.db, server = self.server)
        self.spec = spec
        self.selection = selection
        self.order = order
        self.reader = reader
        self.writer = writer
        self.pk = pk
        self.doc = doc
    
    def copy(self):
        return type(self)(self)
    
    @property
    def schema(self):
        return self.table.schema        
    
    @property
    def _ids(self):
        return [c.name for c in self.table.columns if c.server_default is not None]
    
    def _and(self, doc, keys):
        if len(keys) == 1:
            key = keys[0]
            return self.table.c[key] == doc[key]
        else:
            return sa.and_(*[self.table.c[i] == doc[i] for i in keys])

    def _id(self, doc):
        ### create a filter based on doc
        ids = {i : doc[i] for i in self._ids if i in doc}
        if len(ids):
            return ids
        pks = {i: doc[i] for i in self._pk if i in doc}
        if len(pks):
            return pks
        keys = {i: doc[i] for i in doc if isinstance(doc[i], (int, str, datetime.datetime))}
        if len(keys):
            return keys
        return {}

    @property
    def nullables(self):
        return [c.name for c in self.tbl.columns if c.nullable]
    
    @property
    def non_null(self):        
        ids = self._ids
        return sorted([c.name for c in self.tbl.columns if c.nullable is False and c.name not in ids])
        
    
    def _c(self, expression):
        if isinstance(expression, dict):
            t = self.table.c    
            return sa.and_(*[sa.or_(*[t[k] == i for i in v]) if isinstance(v, list) else t[k] == self._c(v) for k,v in expression.items()]) 
        elif isinstance(expression, (list, tuple)):
            return sa.or_(*[self._c(v) for v in expression])            
        else:
            return expression  
    
    @property
    def c(self):
        return self.table.c
    
    @property
    def _pk(self):
        return ulist(sorted(set(as_list(self.pk))))

    def find(self, *args, **kwargs):
        """
        This returns a table with additional filtering 
        """
        res = self.copy()
        if len(args) == 0 and len(kwargs) == 0:
            res.spec = None
            return res
        elif len(kwargs) > 0 and len(args) == 0:
            e = self._c(kwargs)
        elif len(args) > 0 and len(kwargs) == 0:
            e = self._c(args)
        else:
            raise ValueError('either args or kwargs must be empty, cannot have an "and" and "or" together')            
        if self.spec is None:
            res.spec = e
        else:
            res.spec = sa.and_(self.spec, e)            
        return res

    inc = find
    where = find
    
    def __sub__(self, other):
        if self.selection is None:
            return self.select(self.columns - other)
        elif is_strs(self.selection):
            return self.select(ulist(as_list(self.selection)) - other)
        else:
            raise ValueError('cannot subtract these columns while the selection is non empty, use self.select() to reset selection')
                
    def exc(self, *args, **kwargs):
        """
        This returns a table with additional filtering in q
        
        :Example:
        ---------
        >>> from pyg import * 
        >>> import datetime
        >>> self = get_sql_table(db = 'test', table = 'students', _id = dict(_id = int, created = datetime.datetime), non_null = ['name', 'surname'], nullable =  dict(doc = str, details = str, dob = datetime.date, age = int, grade = float), pk = ['name', 'surname'])
        >>> self.delete()
        >>> assert len(self) == 0         
        >>> self = self.insert(name = 'yoav', surname = 'git')
        >>> self = self.insert(name = 'anna', surname = 'git')
        >>> assert len(self) == 2
        >>> self = self.insert(name = ['opher', 'ayala', 'itamar'], surname = 'gate')
        >>> self[::]        

        dictable[5 x 7]
        _id|created                   |name  |surname|dob |age |grade
        15 |2022-06-25 23:41:55.387000|yoav  |git    |None|None|None 
        16 |2022-06-25 23:41:55.390000|anna  |git    |None|None|None 
        17 |2022-06-25 23:41:55.390000|opher |gate   |None|None|None 
        18 |2022-06-25 23:41:55.390000|ayala |gate   |None|None|None 
        19 |2022-06-25 23:41:55.390000|itamar|gate   |None|None|None 


        >>> self.insert(doc = dict(name = 'josh', surname = 'cohen'))
        type(self[5]['doc'])
        
        
        from pyg import *
        from pyg_mongo._db_cell import _load_asof
        import datetime

        db = partial(get_sql_table, db = 'test', table = 'students', 
                     _id = dict(_id = int, created = datetime.datetime), 
                     non_null = ['name', 'surname'], 
                     nullable =  dict(doc = str, details = str, dob = datetime.date, age = int, grade = float), 
                     pk = ['name', 'surname'], doc = True)        
        table = db()        
        kwargs = dict(name = 'itamar', surname = 'date')
        deleted = False
        qq = None
        doc = _load_asof(table, kwargs, deleted, qq)
        doc = doc.save()
        self = db()
        get_sql_table('students', 'test').delete()
        
        doc1 = db_cell(add_, a = 2, b = 4, name = 'itamar', surname = 'date', db = db).load().go()
        doc2 = db_cell(add_, a = 3, b = 4, name = 'itamar', surname = 'date', db = db).load().go()

        self
        doc = doc1
        self.update_one(doc)

        assert len(db()) == 1        
        doc1._id
        doc2._id        
        
        assert db()[0].data == 
        db().docs()
        doc2.save()
        """
        if len(args) == 0 and len(kwargs) == 0:
            return self
        elif len(kwargs) > 0 and len(args) == 0:
            e = not_(self._c(kwargs))
        elif len(args) > 0 and len(kwargs) == 0:
            e = not_(self._c(args))
        else:
            raise ValueError('either args or kwargs must be empty, cannot have an "and" and "or" together')            
        res = self.copy()
        if self.spec is None:
            res.spec = e
        else:
            res.spec = sa.and_(self.spec, e)            
        return res
    
    
    @property
    def session(self):
        return Session(self.engine)
    
    
    def __len__(self):
        statement = select(func.count()).select_from(self.table)
        if self.spec is not None:
            statement = statement.where(self.spec)
        return list(self.engine.connect().execute(statement))[0][0]
    
    count = __len__

    @property    
    def columns(self):
        return ulist([col.name for col in self.table.columns])

    def select(self, value = None):
        res = self.copy()
        res.selection = value
        return res
    
    def _dock(self, doc, ids = None):
        ids = ids or self._ids
        res = doc[self.doc]
        for i in ids:
            res[i] = doc[i]
        return res

    def docs(self, start = None, stop = None, step = None):
        statement = self.statement()
        if (is_int(start) and start < 0) or (is_int(stop) and stop < 0):
            n = len(self)
            start = n + start if is_int(start) and start < 0 else start                            
            stop = n + stop if is_int(stop) and start < 0 else stop
        if start and self.order is not None:
            statement = statement.offset(start)
            stop = stop if stop is None else stop - start
        if stop is not None:
            statement = statement.limit(1+stop)
        res = list(self.engine.connect().execute(statement))
        rows = res[slice(start, stop, step)]
        rows = [self._read(row) for row in rows]
        rs = dictable(rows, self.columns) ## we want to convert actually to columns...
        return rs        
                
    def __getitem__(self, value):
        if isinstance(value, (str, tuple, list)):
            return self.select(value)
        elif isinstance(value, slice):
            start, stop, step = value.start, value.stop, value.step
            res = self.docs(start = start, stop = stop, step = step)
            if self.doc:
                ids = self._ids
                res = dictable([self._dock(row, ids = ids) for row in res])
            return res
        
        elif is_int(value):
            value = len(self) + value if value < 0 else value
            statement = self.statement()
            if self.order is None:
                res = list(self.engine.connect().execute(statement.limit(value+1)))[value]
            else:
                res = list(self.engine.connect().execute(statement.offset(value).limit(1)))[0]                
            res = self._read(res)
            rtn = Dict(zip(self.columns, res))
            if self.doc:
                rtn = self._dock(rtn)
            return rtn    

    def _enrich(self, doc, columns = None):
        """
        We assume we receive a dict of key:values which go into the db.
        some of the values may in fact be an entire document
        """
        docs = {k : v for k, v in doc.items() if isinstance(v, dict)}
        columns = ulist(self.columns if columns is None else columns)
        res = type(doc)({key : value for key, value in doc.items() if key in columns}) ## These are the only valid columns to the table
        if len(docs) == 0:
            return res
        missing = {k : [] for k in columns if k not in doc}
        for doc in docs.values():
            for m in missing:
                if m in doc:
                    missing[m].append(doc[m])
        found = {k : v[0] for k, v in missing.items() if len(set(v)) == 1}
        conflicted = {k : v for k, v in missing.items() if len(set(v)) > 1}
        if conflicted:
            raise ValueError('got multiple possible values for each of these columns: %s'%conflicted)
        res.update(found)
        return res
                
    def insert_one(self, doc, ignore_bad_keys = False):
        columns = self.columns
        if not ignore_bad_keys:
            bad_keys = {key: value for key, value in doc.items() if key not in columns}
            if len(bad_keys) > 0:
                raise ValueError('cannot insert into db a document with these keys: %s. The table only has these keys: %s'%(bad_keys, columns))
        doc = self._enrich(doc)
        writer = self._writer(None, doc, doc)
        res = Dict({key: self._write(value, writer = writer) for key, value in doc.items() if key in columns})
        if self._pk:
            doc_id = self._id(res)
            ids = self._ids
            tbl = self.inc().inc(**doc_id)
            docs = tbl[::]
            if len(docs) == 0:
                with self.engine.connect() as conn: 
                    conn.execute(self.table.insert(),[res - ids])
                if ids:    
                    latest = tbl[0]
                    doc.update(latest[ids])
            else:
                deleted_docs = docs - ids
                deleted_docs['deleted'] = datetime.datetime.now()
                self.deleted.insert(deleted_docs)
                if len(docs) == 1:
                    latest = docs[0]
                else:
                    latest = docs.sort(ids)[-1]
                    tbl.exc(**tbl._id(latest)).full_delete()
                self.inc(self._id(latest)).update(**(res-ids))
                doc.update(latest[ids])
        else:
            with self.engine.connect() as conn:
                conn.execute(self.table.insert(), [res])
        return doc
    
    
    def update_one(self, doc, upsert = True):
        edoc = {self.doc : doc} if self.doc else doc
        edoc = self._enrich(edoc)
        existing = self.inc().inc(**self._id(edoc))
        n = len(existing)
        if n == 0:
            if upsert is False:
                raise ValueError('no documents found to update %s'%edoc)
            else:
                return self.insert_one(edoc)
        elif self._pk:
            return self.insert_one(edoc)
        elif n == 1:
            writer = self._writer(None, edoc, edoc)
            wdoc = Dict({key: self._write(value, writer = writer) for key, value in edoc.items() if key in self.columns})                
            existing.update(**(wdoc - self._ids))
            res = existing[0]
            res.update(edoc)
            return res
        elif n > 1:
            raise ValueError('multiple documents found matching %s '%edoc)
                
            
    def insert_many(self, docs):
        rs = dictable(docs)
        if len(rs) > 0:
            if self.pk is None:
                columns = self.columns
                rs = dictable([self._enrich(row, columns) for row in rs]) - self._ids
                rows = [{key: self._write(value, kwargs = row) for key, value in row.items()} for row in rs]
                with self.engine.connect() as conn:
                    conn.execute(self.table.insert(), rows)
            else:
                _ = [self.insert_one(doc) for doc in rs]
        return self
    
    def __add__(self, item):
        if is_dict(item) and not is_dictable(item):
            self.insert_one(item)
        else:
            self.insert_many(item)
        return self

        
    def insert(self, data = None, columns = None, **kwargs):
        """
        This allows an insert of either a single row or multiple rows, from anything like 
        >>> self.insert(name = ['father', 'mother', 'childa'], surname = 'common_surname') 
        >>> self.insert(pd.DataFrame(dict(name = ['father', 'mother', 'childa'], surname = 'common_surname')))
        """
        rs = dictable(data = data, columns = columns, **kwargs) ## this allows us to insert multiple rows easily as well as pd.DataFrame
        return self.insert_many(rs)

    def find_one(self, doc = None, *args, **kwargs):
        res = self.find(*args, **kwargs)
        if doc:
            filter_by_doc = self._id(doc)
            if filter_by_doc is not None:
                res = res.find(filter_by_doc)
        if len(res) == 1:
            return res
        elif len(res) == 0:
            raise ValueError('no document found for %s %s %s'%(doc, args, kwargs))
        elif len(res) > 1:
            raise ValueError('multiple documents found for %s %s %s'%(doc, args, kwargs))
            
    
    def _reader(self, reader = None):
        return as_reader(self.reader if reader is None else reader)

    def _read(self, doc, reader = None):
        """
        converts doc from db into something we want
        """
        reader = self._reader(reader)
        res = doc
        if isinstance(doc, sa.engine.row.LegacyRow):
            doc = tuple(doc)
        if isinstance(doc, (list, tuple)):
            return type(doc)([self._read(d, reader) for d in res])
        elif is_str(doc) and doc.startswith('{'):
            res = json.loads(res)
            for r in as_list(reader):
                res = res[r] if is_strs(r) else r(res)
        return res
        
    def _writer(self, writer = None, doc = None, kwargs = None):
        doc = doc or {}
        if writer is None:
            writer = doc.get(_root)
        if writer is None:
            writer = self.writer
        return as_writer(writer, kwargs)
            

    def _write(self, doc, writer = None, kwargs = None):
        if not isinstance(doc, dict):
            return doc
        writer = self._writer(writer, doc, kwargs = kwargs)
        res = doc.copy()
        for w in as_list(writer):
            res = w(res)
        if isinstance(res, dict):
            res = dumps(res)
        return res
    
    def _select(self):
        """
        performs a selection based on self.selection
        """
        if self.selection is None:
            statement = select(self.table)
        elif is_strs(self.selection):               
            c = self.table.c
            selection = [c[v] for v in as_list(self.selection)]
            statement = select(selection).select_from(self.table)
        else: ## user provided sql alchemy selection object
            statement = select(self.selection).select_from(self.table)
        return statement
    
    def statement(self):
        """
        We build a statement from self.spec, self.selection and self.order objects
        A little like:
        
        >>> self.table.select(self.selection).where(self.spec).order_by(self.order)

        """
        statement = self._select()            
        if self.spec is not None:
            statement = statement.where(self.spec)
        if self.order is not None:
            order = self.order
            cols = self.table.columns
            if isinstance(order, (str,list)):
                order = {o: 1 for o in as_list(order)}           
            statement = statement.order_by(*[_orders[v](cols[k]) for k, v in order.items()])
        return statement
    
    def update(self, **kwargs):
        if len(kwargs) == 0:
            return self
        statement = self.table.update()
        if self.spec is not None:
            statement = statement.where(self.spec)
        statement = statement.values(kwargs)
        with self.engine.connect() as conn:
            conn.execute(statement)
        return self
    
    set = update

    def full_delete(self):
        statement = self.table.delete()
        if self.spec is not None:
            statement = statement.where(self.spec)
        with self.engine.connect() as conn:
            conn.execute(statement)
        return self

    def delete(self, **kwargs):
        res = self.inc(**kwargs)
        if self._pk: ## we first copy the existing data out to deleted db
            docs = res[::]
            docs['deleted'] = datetime.datetime.now()
            self.deleted.insert(docs)
        res.full_delete()
        return self
        
    def sort(self, order = None):
        if order is None:
            return self
        else:
            res = self.copy()
            res.order = order
            return res
    
    def distinct(self, *keys):
        if len(keys) == 0 and self.selection is not None:
            keys = as_list(self.selection)
        session = Session(self.engine)
        cols = [self.table.columns[k] for k in keys]
        query = session.query(*cols)
        if self.spec is not None:
            query = query.where(self.spec)        
        res = query.distinct().all()
        if len(keys)==1:
            res = [row[0] for row in res]
        return res
    
    def __repr__(self):
        return '%(db)s.%(table)s, %(n)i records\n%(statement)s'%dict(db = self.db, table = self.table.name, n = len(self), statement = str(self.statement()))
                
    def _is_deleted(self):
        return self.db.startswith('deleted_')

    @property
    def deleted(self):
        if self._is_deleted():
            return self.distinct('deleted')
        else:        
            db_name = 'deleted_' + self.db
            res = get_sql_table(table = self.table, db = db_name, non_null = dict(deleted = datetime.datetime), server = self.server)
            res.spec = self.spec
            res.order = self.order
            res.pk = None
            return res
                
    @property
    def address(self):
        """
        :Returns:
        ---------
        tuple
            A unique combination of the client addres, db name and collection name, identifying the collection uniquely.

        """
        return ('server', self.server or _server()), ('db', self.db), ('table', self.table.name)


