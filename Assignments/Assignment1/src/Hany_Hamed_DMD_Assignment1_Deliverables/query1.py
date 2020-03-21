# ---------------------------------------------------------------------------------------------------
# Author: Hany Hamed
# Description: Source file for 1st query codes [for Assignment 1 for DMD2 course]
# Sources:
# (1). https://stackoverflow.com/questions/4421207/how-to-get-the-last-n-records-in-mongodb
# (2). https://stackoverflow.com/questions/7651064/create-an-isodate-with-pymongo
# (3). https://stackoverflow.com/questions/26984799/find-duplicate-records-in-mongodb
# (4). https://dzone.com/articles/basic-aggregation-mongodb-21
# (5). https://www.compose.com/articles/aggregations-in-mongodb-by-example/
# (6). https://www.isummation.com/blog/perform-inner-join-in-mongodb-using-lookup-aggregation-operator/
# ---------------------------------------------------------------------------------------------------

from utils import *
from datetime import datetime
import pprint
import json
from bson.json_util import dumps


class Query1:
    def __init__(self,db):
        self.db = db
        self.pp = pprint.PrettyPrinter()
        self.query_number = 1


    def execute(self, params=None):
        print("### Executing Query {:}".format(self.query_number))
        self._query()
        print("### Finished Execution of Query {:}".format(self.query_number))

    def _query(self,params=None):
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
                            "$unwind":"$customer"
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
                        {"$project": {"customer" : 1, "inventory_id":1, "_id":0} }, 
                        {
                            "$lookup":
                            {
                            "from": "inventory",
                            "localField": "inventory_id",
                            "foreignField": "inventory_id",
                            "as": "inventory"
                            }
                        },
                        {
                            "$unwind":"$inventory"
                        }, 
                        {"$project": {"customer" : 1, "film_id":"$inventory.film_id"} }, 
                        {
                            "$lookup":
                            {
                            "from": "film_category",
                            "localField": "film_id",
                            "foreignField": "film_id",
                            "as": "film_category"
                            }
                        },
                        {
                            "$unwind":"$film_category"
                        }, 
                        {"$project": {"customer" : 1, "category_id":"$film_category.category_id"} }, 
                        {
                            "$group" : 
                            {
                                # "_id": "$customer.customer_id",   # For getting only the customer id not the whole info
                                "_id": "$customer",
                                "count": { "$sum": 1 },
                                "rest":
                                {
                                    "$addToSet":
                                    {
                                        "customer":"$customer",
                                        "category_id":"$category_id"
                                    }
                                },
                            } 
                        },
                        {
                            "$match": 
                            {
                                "count" : {"$gte": 2}
                            } 
                        },
                        {"$sort": {"count" : -1} },
                        {"$project": {"customer":"$_id", "count_diff": {"$size": "$rest"}, "num_rented_movies_in_the_current_year":"$count", "_id" : 0}},
                        {
                            "$match": 
                            {
                                "count_diff" : {"$gte": 2}
                            } 
                        }   
        ]
        a = self.db.rental.aggregate(my_pipeline)
        results = []
        for i in a:
            tmp_dict = {i["customer"]["customer_id"]:i}
            results.append(tmp_dict)
            # results.append(i)
            print("---------------------------------")
            self.pp.pprint(tmp_dict)
            print("---------------------------------")
        print("# {:} Customers that satisfied the query".format(len(results)))
        print("### Writing the report to report_query1.json file")
        with open("report_query1.json","w") as f:
            for i in results:
                # Source: https://stackoverflow.com/questions/16586180/typeerror-objectid-is-not-json-serializable
                tmp = dumps(i)
                json.dump(tmp,f)


if __name__ == "__main__":
    from utils import *

    settings = get_settings()

    client_mngdb, db_mngdb = init_mongodb(settings["host"], settings["database"])
    q = Query1(db_mngdb)
    q.execute()