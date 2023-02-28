# pyg-mongo

pip install from https://pypi.org/project/pyg-mongo/
pyg-mongo is an evolution from ArcticDB available here: https://github.com/man-group/arctic.
pyg-mongo introduces three new concepts that help you work with mongodb

* The q query generation, making it easy to filter mongo documents.  
* The mongo_cursor makes read/write into mongo of complicated object seemless
* the mongo_table with pk specified, implements an audited primary-keyed table 

## The q-query
For those of us who used TinyDB or even sqlalchemy, filtering mongodb for documents is a huge hassle.
You can use q to write those complicated Mongo filter dicts:

```
>>> from pyg_mongo import q
>>> q.age == 3
{"age": {"$eq": 3}}

>>> (q.age > 3) & (q.gender == 'm')
{"$and": [{"age": {"$gt": 3}}, {"gender": {"$eq": "m"}}]}


>>> q(q.age < 10, name = re.compile('ben'), surname = ['smith', 'jones'])
$and:
    {"age": {"$lt": 10}}
    {"name": {"regex": "ben"}}
    {"surname": {"$in": ["smith", "jones"]}}
```

## The mongo_cursor

mongo_cursor uses q under the hood, making document filtering easy. The mongo_cursor also pre-process on both read and write to make writing 

* numpy objects/primitives easy to read/write
* objects are jsonified/cast into bytes (pandas) so that the can be stored directly
* allows the user to pre-save documents to csv/parquet/npy/pickle files while the metadata is saved in MongoDB

The post-reading process then makes the whole experience transparent to the user.

```
>>> table = mongo_table('table','db').delete_many() # create table and drop any existing records
>>> doc = dict( a = np.array([1,2,3]), 
                s = pd.Series([1,2,3]), 
                df = pd.DataFrame(dict(a = [1,2], b = [3,4])))
>>> doc = table.insert_one(doc)
>>> len(table)
1
>>> read_doc = table[0]
>>> read_doc['a']
array([1, 2, 3])
>>> read_doc['s']
0    1
1    2
2    3
dtype: int64
```

Saving the array/pd.DataFrame into MongoDB is not that useful though... 
Once stored as a bson inside Mongo it cannot be queried easily and other applications cannot access it easily.
mongo_cursor supports writing the actual data to files in pickle/npy/parquet/csv formats while the document itself sits inside Mongo. 
This allows us to create applications where: 

* ZeroMQ/Kafka saves tick data directly to npy files and 
* These files are accessible to the user immediately and transparently through mongo_cursor.

Splitting data & metadata in this ways allows us to use the strength of Mongo for queries while delegating low level operations to existing tried-and-tested file-based technology.

```
>>> doc = dict(a = np.array([1,2,3]), s = pd.Series([1,2,3]), df = pd.DataFrame(dict(a = [1,2], b = [3,4])), root = 'c:/temp.parquet')
>>> table.insert_one(doc)
>>> assert os.path.isfile('c:/temp/df.parquet') and os.path.isfile('c:/temp/s.parquet') and os.path.isfile('c:/temp/a.npy')
# you can even specify the root to depend on keys in the document...

>>> doc = dict(name = 'james', surname = 'smith', a = np.array([1,2,3]), s = pd.Series([1,2,3]), df = pd.DataFrame(dict(a = [1,2], b = [3,4])), root = 'c:/temp/%name_%surname.parquet')
>>> table.insert_one(doc)
>>> assert os.path.isfile('c:/temp/james_smith/df.parquet')
```

## The mongo_table with primary keys
```
>>> table = mongo_table(db = 'school', table = 'students', pk = ['year', 'name', 'surname'])
>>> table.reset.delete_many()
>>> table.insert_one(dict(year = 1, name = 'abe', surname = 'abraham', age = 6, weight = 35, height = 1.34, attendance = pd.Series([0,1,0], drange(-2)), version = 1))
>>> assert len(table) == 1 

# since we got the height wrong... let us fix this:
>>> table.insert_one(dict(year = 1, name = 'abe', surname = 'abraham', age = 6, weight = 35, height = 1.35, attendance = pd.Series([0,1,0], drange(-2)), version = 2))
>>> assert len(table) == 1  # primary keys insertion. The old record is marked as deleted and table thinks there is still only one doc

## here come new students
>>> table.insert_one(dict(year = 1, name = 'ben', surname = 'bradshaw', age = 7, weight = 40, height = 1.14, attendance = pd.Series([0,1,0], drange(-2))))
>>> table.insert_one(dict(year = 1, name = 'clive', surname = 'cohen', age = 6.2, weight = 20, height = 1.34, attendance = pd.Series([1,1,0], drange(-2))))
>>> table.insert_one(dict(year = 2, name = 'dana', surname = 'dowe', age = 8.2, weight = 25, height = 1.04, attendance = pd.Series([1,1,1], drange(-2))))

>>> assert len(table) == 4
>>> assert table.year == [1, 2] #distinct
>>> table[::] - 'attendance'

Out[117]: 
dictable[4 x 9]
year|name |surname |pk                         |age|version|_id                     |height|weight
1   |abe  |abraham |['name', 'surname', 'year']|6  |2      |61b019ec1180e336cb2d845a|1.35  |35    
1   |ben  |bradshaw|['name', 'surname', 'year']|7  |None   |61b019ec1180e336cb2d845c|1.14  |40    
1   |clive|cohen   |['name', 'surname', 'year']|6.2|None   |61b019ec1180e336cb2d845d|1.34  |20    
2   |dana |dowe    |['name', 'surname', 'year']|8.2|None   |61b019ec1180e336cb2d845e|1.04  |25    

>>> assert len(table.exc(year = 2)) == 3
```

There is more to pyg-mongo but this is a good taster.
