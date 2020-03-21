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
# ---------------------------------------------------------------------------------------------------

from utils import *
from datetime import datetime
import pprint
import csv
import numpy as np


class Query2:
    def __init__(self,db):
        self.db = db
        self.pp = pprint.PrettyPrinter()
        self.query_number = 2

    def execute(self, params=None):
        print("### Executing Query {:}".format(self.query_number))
        self._query()
        print("### Finished Execution of Query {:}".format(self.query_number))

    def get_actors(self):
        a = self.db.actor.find()
        self.actor_full = {}

        for i in a:
            self.actor_full[i["actor_id"]] = i
        # for i in self.actor_full.keys():
        #     print("# {:} -> {:}".format(i,self.actor_full[i]))

    def _query(self,params=None):
        print("### Getting the actors ")
        self.get_actors()
        print("### Got the actors ")
        print("### Starting getting the results of the query")
        my_pipeline = [                       
                        {
                            "$lookup":
                            {
                            "from": "film_actor",
                            "localField": "film_id",
                            "foreignField": "film_id",
                            "as": "actor2"
                            }
                        },
                        {
                            "$unwind":"$actor2"
                        },
                        {
                            "$group" : 
                            {
                                "_id": {"actor1":"$actor_id", "actor2":"$actor2.actor_id"},
                                "num_films":
                                {
                                    "$sum":1
                                }
                            } 
                        },
                        {"$sort": {"_id.actor1" : 1, "_id.actor2" : 1} },
                        {"$project": {"co_actors":"$_id", "num_films": "$num_films","_id" : 0} }    
        ]
        a = self.db.film_actor.aggregate(my_pipeline)
        results = []
        actors = []

        for i in a:
            results.append(i)
            actors.append(i["co_actors"]["actor1"])
            # print("---------------------------------")
            # self.pp.pprint(i)
            # print("---------------------------------")
        # print(len(results))
        print("### Finished getting the results of the query")
        print("### Starting write the report in form of table in csv")
        actors = list(set(actors))
        # Actor1 as row and Actor2 as column
        data = np.zeros((len(actors)+1, len(actors)+1),dtype=int)
        for i in range(1,len(actors)+1):
            data[0][i] = actors[i-1]
            data[i][0] = actors[i-1]
        for i in results:
            row_index = int(i["co_actors"]["actor1"])
            column_index = int(i["co_actors"]["actor2"])
            data[row_index][column_index] = i["num_films"]
        np.savetxt("report_query2.csv", data, delimiter=",", fmt='%d')
        print("### Finished Writing to report_query2.csv file")





if __name__ == "__main__":
    from utils import *

    settings = get_settings()

    client_mngdb, db_mngdb = init_mongodb(settings["host"], settings["database"])
    q = Query2(db_mngdb)
    q.execute()