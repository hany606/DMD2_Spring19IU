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
# (7). https://docs.mongodb.com/manual/reference/operator/aggregation/unwind/
# (8). https://realpython.com/python-csv/
# (9). https://stackoverflow.com/questions/209840/convert-two-lists-into-a-dictionary
# (10). https://stackabuse.com/reading-and-writing-json-to-a-file-in-python/
# ---------------------------------------------------------------------------------------------------

# The same as query 2 to get the table of costs then
#   Iterate all over the ids in for loop as changing the target while the source(the root tree is the same always)
#   Then with optimal cost array which is initialized with -1, just add in it the costs while exploring if the value of the array was -1 (didn't change)
#   In exploring all the tree or with a counter that count the modifications if it reached the number of actors then it converges to an optimal (low cost path -> bacon's law)
#    This optimization is not possible, the possible is to cumulative


from utils import *
from datetime import datetime
import pprint
import json
import numpy as np


class Query5:
    def __init__(self,db):
        self.db = db
        self.pp = pprint.PrettyPrinter()
        self.query_number = 5

    def execute(self, params=None):
        print("### Executing Query {:}".format(self.query_number))
        self._query(params=params)
        print("### Finished Execution of Query {:}".format(self.query_number))

    def BFS(self, actor_id):
        print("------------ Actor_root")
        for i in range(1,len(self.separation_degrees)):
            if(i == actor_id):
                self.separation_degrees[i] = 0
                continue

            if(self.separation_degrees[i] == -1):
                self.separation_degrees[i] = self.costs_matrix[actor_id][i]
                self.modification_counter -= 1
                if(self.modification_counter == 0):
                    return
                continue
            if(self.modification_counter == 0):
                return

            self.BFS(i)
        return
    def _query(self,params=None):
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
        self.costs_matrix = np.zeros((len(actors)+1, len(actors)+1),dtype=int)
        self.separation_degrees = [-1 for i in range(len(actors))]
        self.modification_counter = len(actors)
        for i in range(1,len(actors)):
            self.costs_matrix[0][i] = actors[i-1]
            self.costs_matrix[i][0] = actors[i-1]

        for i in results:
            row_index = int(i["co_actors"]["actor1"])
            column_index = int(i["co_actors"]["actor2"])
            if(row_index == column_index):
                continue
            self.costs_matrix[row_index][column_index] = i["num_films"]
        
        print("### Starting calculating the degrees of seperation")
        self.BFS(params["source_actor_id"])
        print("### Finished calculating the degrees of seperation")
            


if __name__ == "__main__":
    from utils import *

    settings = get_settings()

    client_mngdb, db_mngdb = init_mongodb(settings["host"], settings["database"])
    q = Query5(db_mngdb)
    q.execute(params={"source_actor_id":10})