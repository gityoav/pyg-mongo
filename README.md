# pyg-mongo

pip install from https://pypi.org/project/pyg-mongo/

pyg-mongo introduces three new concepts that help you work with mongodb

* The q query generating engine, making easy to filter mongo documents
* The mongo_table makes read/write into mongo of complicated object seemless
* the mongo_table with pk specified, implements an audited primary-keyed table 

## The q-query
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

## The mongo_table

mongo_table uses q under the hood, making filtering easy. It also pre-process on both read and write to make:

* number primitives (such as float32) that cannot be stored in MongoDB are converted to normal primitives
* objects are jsonified/cast into bytes (pandas) so that the can be stored directly
* allows the user to pre-save documents to csv/parquet/npy files while the metadata is saved in MongoDB

The post-reading process then makes the whole experience transparent to the user.

```
>>> table = mongo_table('table','db').delete_many() # create table and drop any existing records
>>> doc = dict(a = np.array([1,2,3]), s = pd.Series([1,2,3]), df = pd.DataFrame(dict(a = [1,2], b = [3,4])))
>>> doc = table.insert_one(doc)
>>> len(table)
1
>>> read_doc = table[0]
>>> read_doc['a']
array([1, 2, 3])

```
Here is how we save into a directory...

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
