# ---------------------------------------------------------------------------------------------------
# Author: Hany Hamed
# Description: Main code that add using the queries and executing them [for Assignment 1 for DMD2 course]
# Sources:
# - https://blog.exploratory.io/an-introduction-to-mongodb-query-for-beginners-bd463319aa4c
# - https://www.w3schools.com/python/python_mongodb_query.asp
# ---------------------------------------------------------------------------------------------------

from Queries import Queries
from utils import *

settings = get_settings()

client_mngdb, db_mngdb = init_mongodb(settings["host"], settings["database"])
q = Queries(db_mngdb)
# collection = db_mngdb.customer

# # my_query = {{},{ "_id": 0, "first_name": 1}}

# my_query = {}
# my_fields = { "_id": 0, "first_name": 1}


# document = collection.find(my_query,my_fields)

# for x in list(document):
#     print(x)
q.query(1)




client_mngdb.close()