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
# (7). https://docs.mongodb.com/manual/reference/operator/aggregation/unwind/
# (8). https://realpython.com/python-csv/
# (9). https://stackoverflow.com/questions/209840/convert-two-lists-into-a-dictionary
# (10). https://stackabuse.com/reading-and-writing-json-to-a-file-in-python/
# ---------------------------------------------------------------------------------------------------

from utils import *
from datetime import datetime
import pprint
import json
import numpy as np


class Query3:
    def __init__(self,db):
        self.db = db
        self.pp = pprint.PrettyPrinter()
        self.query_number = 3

    def execute(self, params=None):
        print("### Executing Query {:}".format(self.query_number))
        self._query()
        print("### Finished Execution of Query {:}".format(self.query_number))

    def _query(self,params=None):
        print("### Starting getting the films and their category")
        # The query has been seperated to two queries as from the experiment when they were one query it took more time as there is more columns each joint
        my_pipeline = [                       
            {
                "$lookup":
                {
                "from": "category",
                "localField": "category_id",
                "foreignField": "category_id",
                "as": "category"
                }
            },
            {
                "$unwind":"$category"
            },
            {
                "$lookup":
                {
                "from": "film",
                "localField": "film_id",
                "foreignField": "film_id",
                "as": "film"
                }
            },
            {
                "$unwind":"$film"
            },
            {"$sort":{"film_id":1}},
            {
                "$project": {"_id":0, "category.name":1, "category_id":1, "film_id":1, "film.title":1}
            }
        ]
        a = self.db.film_category.aggregate(my_pipeline)
        # Zero indexed
        films = []
        for i in a:
            films.append(i)
        #     print("---------------------------------")
        #     self.pp.pprint(i)
        #     print("---------------------------------")
        # print(len(films))
        # 1 indexed
        films_full = [{} for i in range((len(films)+1))]
        print("### Finished getting the films and their category of the query")
        print("### Starting getting the number of times the films is rented")
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
                "$project": {"customer_id":1, "rental_id":1, "inventory_id":1}
            },
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
            {
                "$project": {"customer_id":1, "inventory.film_id":1}
            },
            {
                "$group" : 
                {
                    "_id": "$inventory.film_id",
                    "count": { "$sum": 1 } 
                } 
            },
            {"$sort": {"_id":1,"count" : 1} },
            {"$project": {"film_id":"$_id", "count":"$count", "_id" : 0} }    
        ]
        # t = self.db.rental.find(my_query,my_fields)
        a = self.db.rental.aggregate(my_pipeline)
        results = []

        for i in a:
            results.append(i)
            film_id = int(i["film_id"])
            films_full[film_id] = {"film": films[film_id-1], "count":int(i["count"])}
            # print("---------------------------------")
            # self.pp.pprint(i)
            # print("---------------------------------")
        for film_id in range(1,len(films_full)):
            if(len(films_full[film_id]) == 0):
                # print(film_id)
                films_full[film_id] = {"film": films[film_id-1], "count":0}

        # print(len(results))
        print("### Writing the report to report_query3.json file")
        with open("report_query3.json","w") as f:
            json.dump(films_full,f,indent=4)




if __name__ == "__main__":
    from utils import *

    settings = get_settings()

    client_mngdb, db_mngdb = init_mongodb(settings["host"], settings["database"])
    q = Query3(db_mngdb)
    q.execute()