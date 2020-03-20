# ---------------------------------------------------------------------------------------------------
# Author: Hany Hamed
# Description: Source file for queries codes [for Assignment 1 for DMD2 course]
# Sources:
# (1). Forgot the link
# (2). https://stackoverflow.com/questions/7651064/create-an-isodate-with-pymongo
# (3). https://stackoverflow.com/questions/26984799/find-duplicate-records-in-mongodb
# (4). https://dzone.com/articles/basic-aggregation-mongodb-21
# (5). https://www.compose.com/articles/aggregations-in-mongodb-by-example/
# ---------------------------------------------------------------------------------------------------

from utils import *
from datetime import datetime


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
            # Source (1)
            # Current year is the most recent record in the table rental from the rental date
            my_query = {}
            my_fields = {"_id":0, "rental_date": 1}
            my_limit = 1
            t = self.db.rental.find(my_query,my_fields).sort("rental_date", -1).limit(my_limit)
            current_year = list(t)[0]["rental_date"].year
            print(current_year)
            return current_year

        # Get all the customers that rented at least two different categories during the current year*.
        current_year = str(get_current_year())
        # Source (2,3)
        from_dt = datetime.strptime('{:}-01-01'.format(current_year),'%Y-%m-%d')
        to_dt = datetime.strptime('{:}-12-31'.format(current_year),'%Y-%m-%d')
        my_pipeline = [
                        {
                            "$match": 
                            {
                                "rental_date": 
                                {
                                    "$gte":from_dt,
                                    "$lte": to_dt,
                                }
                            } 
                        }, 
                        {
                            "$group" : 
                            {
                                "_id": "$customer_id",
                                "count": { "$sum": 1 } 
                            } 
                        },
                        {
                            "$match": 
                            {
                                "count" : {"$gte": 2}
                            } 
                        }, 
                        {"$sort": {"count" : -1} },
                        {"$project": {"customer_id" : "$_id", "num_times":"$count", "_id" : 0} }    
        ]
        # t = self.db.rental.find(my_query,my_fields)
        a = self.db.rental.aggregate(my_pipeline)
        results = []
        for i in a:
            results.append(i)
            print(i)
        print(len(results))
        
    def _query2(self,params=None):
        pass

    def _query3(self,params=None):
        pass
    
    def _query4(self,params=None):
        pass
    
    def _query5(self,params=None):
        pass