from pyg_mongo import periodic_cell
from pyg_base import dt

def test_periodic_cell():
     c = periodic_cell(lambda a: a + 1, a = 0) 
     assert c.run() 
     c = c.go() 
     assert c.data == 1 
     assert not c.run()     
     c.updated = dt(2000)
     assert c.run()
     c.end_date = dt(2010)
     assert c.run()
     c.end_date = dt(2000,1,2)
     assert not c.run()

    