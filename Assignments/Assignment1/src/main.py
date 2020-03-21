# ---------------------------------------------------------------------------------------------------
# Author: Hany Hamed
# Description: Main code that add using the queries and executing them [for Assignment 1 for DMD2 course]
# Sources:
# - https://blog.exploratory.io/an-introduction-to-mongodb-query-for-beginners-bd463319aa4c
# - https://www.w3schools.com/python/python_mongodb_query.asp
# ---------------------------------------------------------------------------------------------------

from Queries import Queries
from utils import *
import time

settings = get_settings()

client_mngdb, db_mngdb = init_mongodb(settings["host"], settings["database"])
q = Queries(db_mngdb)
while(True):
    q_num = int(input("Enter number of the query:"))
    if(q_num == -1 or q_num > 5):
        print("Finish !!")
        break
    params = {}
    if(q_num == 4):
        params["customer_id"] = int(input("Enter Cutomer_id: "))
    if(q_num == 5):
        params["source_actor_id"] = int(input("Enter Actor_id: "))
        
    time1 = time.time()
    q.query(q_num, params=params)
    print("############ Statistics ############")
    print("Time taken to finish to {:}th query ::: {:.4f}s".format(q_num,time.time()-time1))
    print("####################################")

client_mngdb.close()