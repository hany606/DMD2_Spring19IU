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

### The metric for this query:
# The best 5 categories for him in a descending order as it is known by counting the number of films that he/she rent all over his/her time as a customer


from utils import *
from datetime import datetime
import pprint
import json
import numpy as np


class Query4:
    def __init__(self,db):
        self.db = db
        self.pp = pprint.PrettyPrinter()
        self.query_number = 4

    def execute(self, params=None):
        print("### Executing Query {:}".format(self.query_number))
        self._query(params=params)
        print("### Finished Execution of Query {:}".format(self.query_number))

    def _query(self,params=None):
        print("Customer: with id #{:}".format(params["customer_id"]))
        print("### Starting getting the best 5 categories that this customer like in descending order")
        # The query has been seperated to two queries as from the experiment when they were one query it took mor time as there is more columns each joint
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
                    "customer_id":
                    {
                        "$eq":params["customer_id"]
                    }
                }
            },
            {
                "$project": {"inventory_id":1, "_id":0}
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
                "$project": {"inventory.film_id":1, "_id":0}
            },
            {
                "$lookup":
                {
                "from": "film",
                "localField": "inventory.film_id",
                "foreignField": "film_id",
                "as": "film"
                }
            },
            {
                "$unwind":"$film"
            }, 
            {
                "$project": {"film.film_id":1, "_id":0}
            },
            {
                "$lookup":
                {
                "from": "film_category",
                "localField": "film.film_id",
                "foreignField": "film_id",
                "as": "film_category"
                }
            },
            {
                "$unwind":"$film_category"
            },
            {
                "$lookup":
                {
                "from": "category",
                "localField": "film_category.category_id",
                "foreignField": "category_id",
                "as": "category"
                }
            },
            {
                "$unwind":"$category"
            }, 
            {
                "$project": {"category.name":1, "_id":0}
            },
            {
                "$group" : 
                {
                    "_id": "$category.name",
                    "count": { "$sum": 1 } 
                } 
            },
            {"$sort": {"count" : -1} },
            {"$limit":5},
            {"$project": {"category":"$_id", "count":"$count", "_id" : 0} }    
        ]
        a = self.db.rental.aggregate(my_pipeline)
        best_categories = []
        for i in a:
            best_categories.append(i["category"])
        #     print("---------------------------------")
        #     self.pp.pprint(i)
        #     print("---------------------------------")
        # print(len(best_categories))
        print("Best 5 Categories: ", best_categories)
        print("### Finished getting the films and their category of the query")
        print("### Starting getting the recommendations for new films according to his/her history of categories")
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
                "$match":
                {
                    "category.name":
                    {
                        "$in":best_categories
                    }
                }
            },
            {
                "$project": {"_id":0,"film_id":1, "category.name":1}
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
            {
                "$project": {"_id":0, "film_id":1, "film.title":1, "category.name":1}
            },
            {
                "$lookup":
                {
                "from": "inventory",
                "localField": "film_id",
                "foreignField": "film_id",
                "as": "inventory"
                }
            },
            {
                "$unwind":"$inventory"
            }, 
            {
                "$project": {"_id":0, "inventory.inventory_id":1, "film_id":1, "film.title":1, "category.name":1}
            },
            {
                "$lookup":
                {
                "from": "rental",
                "localField": "inventory.inventory_id",
                "foreignField": "inventory_id",
                "as": "rental"
                }
            },
            {
                "$unwind":"$rental"
            }, 
            {
                "$project": {"_id":0, "rental.rental_id":1, "rental.customer_id":1, "film_id":1, "film.title":1, "category.name":1}
            },
            {
                "$match":
                {
                    "rental.customer_id":
                    {
                        "$ne":params["customer_id"]
                    }
                }
            },
            {
                "$project": {"_id":0, "film_id":1, "film.title":1, "category.name":1}
            },
            {
                "$group":
                {
                    "_id":{"film_id": "$film_id", "film_title":"$film.title", "category_name":"$category.name"}
                }
            },
            {"$limit":10},
            {
                "$project": {"film_id":"$_id.film_id", "film_title":"$_id.film_title", "category_name":"$_id.category_name", "_id":0}
            },

        ]
        a = self.db.film_category.aggregate(my_pipeline)
        recommendations = []
        for i in a:
            recommendations.append(i)
        #     print("---------------------------------")
        #     self.pp.pprint(i)
        #     print("---------------------------------")
        # print(len(recommendations))
        print("### Finished getting the recommendations for the films according to their categories")
        with open("report_query4.txt", "w") as f:
            print("# Recommendations #####################")
            f.write("# Recommendations #####################\n")
            for i,r in enumerate(recommendations):
                print("(*). {:} from {:} category".format(r["film_title"],r["category_name"]))
                f.write("(*). {:} from {:} category\n".format(r["film_title"],r["category_name"]))
            print("#######################################")
            f.write("#######################################\n")

if __name__ == "__main__":
    from utils import *

    settings = get_settings()

    client_mngdb, db_mngdb = init_mongodb(settings["host"], settings["database"])
    q = Query4(db_mngdb)
    q.execute(params={"customer_id":524})