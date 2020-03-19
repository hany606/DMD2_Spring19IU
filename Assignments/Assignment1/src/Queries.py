from utils import *

class Queries:
    def __init__(self,db):
        self.db = db

    def query(self, q_num, params=None):
        queries = [self._query1, self._query2, self._query3, self._query4, self._query5]
        print("### Executing Query {:}".format(q_num))
        result = queries[q_num-1](params)
        print("### Finished Execution of Query {:}".format(q_num))

    def _query1(self,params=None):
        def get_current_year():
            t = self.db.rental.find().limit(1).sort({'_id':-1})
            # Current year is the most recent record in the table rental
            # print(t)
        # Get all the customers that rented at least two different categories during the current year*.
        # get_current_year()
        # t = self.db.rental.find()

    def _query2(self,params=None):
        pass

    def _query3(self,params=None):
        pass
    
    def _query4(self,params=None):
        pass
    
    def _query5(self,params=None):
        pass