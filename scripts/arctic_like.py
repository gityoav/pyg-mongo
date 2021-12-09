### a simply arctic replacement...
from pyg_mongo import *
from pyg_base import * 
import pandas as pd; import numpy as np

### treating mongo as a simple put in, get out:
    
table = mongo_table('markets', 'futures', pk = ['market', 'item'])
table.reset.drop()

for market in ['CLN', 'EDC', 'TNC', 'GLN']:
    rtn = pd.Series(np.random.normal(0,1,10000), drange('-9999b', 0, '1b'))    
    adj = rtn.cumsum()
    vol = (rtn**2).ewm(18).mean()**0.5
    table.insert_one(db_cell(data = rtn, market = market, item = 'rtn'))
    table.insert_one(db_cell(data = vol, market = market, item = 'vol'))
    table.insert_one(db_cell(data = adj, market = market, item = 'adj'))
    

### access:
get_data('markets', 'futures', market = 'CLN', item = 'vol')
table.find(market = 'CLN', item = 'vol')[0].data

### looks the same but files saved locally:
table = mongo_table('markets', 'futures', pk = ['market', 'item'], writer = 'c:/temp/%market/%item.parquet')
for market in ['CLN', 'EDC', 'TNC', 'GLN']:
    rtn = pd.Series(np.random.normal(0,1,10000), drange('-9999b', 0, '1b'))    
    adj = rtn.cumsum()
    vol = (rtn**2).ewm(18).mean()**0.5
    table.insert_one(db_cell(data = rtn, market = market, item = 'rtn'))
    table.insert_one(db_cell(data = vol, market = market, item = 'vol'))
    table.insert_one(db_cell(data = adj, market = market, item = 'adj'))

### all these work:
get_data('markets', 'futures', market = 'CLN', item = 'vol') ### will fetch first if in local cache
load_data('markets', 'futures', market = 'CLN', item = 'vol') ### force a reload from Mongo
table.find(market = 'CLN', item = 'vol')[0].data
pd.read_parquet('c:/temp/CLN/vol/data.parquet')


### dragons beware... you can save multiple items...
table = mongo_table('markets', 'futures', pk = ['market', 'item'])

for market in ['CLN', 'EDC', 'TNC', 'GLN']:
    rtn = pd.Series(np.random.normal(0,1,10000), drange('-9999b', 0, '1b'))    
    adj = rtn.cumsum()
    vol = (rtn**2).ewm(18).mean()**0.5
    table.insert_one(db_cell(vol = vol, rtn = rtn, adj = adj, market = market, item = 'all_data'))

all_data = get_cell('markets', 'futures', market = 'CLN', item = 'all_data')
all_data.vol


table = mongo_table('table', 'db')
table.insert_one(dict(attendance = pd.Series([1,2,3], [4,5,6]), root = 'c:/temp/example.parquet'))
table['attendance']




