# ---------------------------------------------------------------------------------------------------
# Author: Hany Hamed
# Description: Source file for 1st query codes [for Assignment 1 for DMD2 course]
# Sources:
# (1). Forgot the link
# (2). https://stackoverflow.com/questions/7651064/create-an-isodate-with-pymongo
# (3). https://stackoverflow.com/questions/26984799/find-duplicate-records-in-mongodb
# (4). https://dzone.com/articles/basic-aggregation-mongodb-21
# (5). https://www.compose.com/articles/aggregations-in-mongodb-by-example/
# (6). https://www.isummation.com/blog/perform-inner-join-in-mongodb-using-lookup-aggregation-operator/
# ---------------------------------------------------------------------------------------------------

from utils import *
from datetime import datetime
import pprint


class Query5:
    def __init__(self,db):
        self.db = db
        self.pp = pprint.PrettyPrinter()

    def execute(self, params=None):
        print("### Executing Query {:}".format(1))
        self._query1()
        print("### Finished Execution of Query {:}".format(1))

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
                            "$lookup":
                            {
                            "from": "customer",
                            "localField": "customer_id",
                            "foreignField": "customer_id",
                            "as": "customer"
                            }
                        },
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
                        {"$project": {"customer" : "$customer"} }, 
                        {
                            "$group" : 
                            {
                                # "_id": "$customer.customer_id",   # For getting only the customer id not the whole info
                                "_id": "$customer",
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
                        {"$project": {"customer":"$_id", "num_rented_movies_in_the_current_year":"$count", "_id" : 0} }    
        ]
        # t = self.db.rental.find(my_query,my_fields)
        a = self.db.rental.aggregate(my_pipeline)
        results = []
        for i in a:
            results.append(i)
            print("---------------------------------")
            self.pp.pprint(i)
            print("---------------------------------")
        print(len(results))


if __name__ == "__main__":
    from utils import *

    settings = get_settings()

    client_mngdb, db_mngdb = init_mongodb(settings["host"], settings["database"])
    q1 = Query5(db_mngdb)
    q1.execute()