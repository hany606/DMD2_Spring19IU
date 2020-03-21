# ---------------------------------------------------------------------------------------------------
# Author: Hany Hamed
# Description: Utilities and shared code for all the other files [for Assignment 1 for DMD2 course]
# Sources:
# - https://www.w3schools.com/python/python_mongodb_getstarted.asp
# ---------------------------------------------------------------------------------------------------

from pymongo import MongoClient
import pymongo
import json

def get_settings(file_name="settings.json"):
    with open(file_name) as f:
        settings = json.load(f)
    return settings

def init_mongodb(host, database):
    client = MongoClient("mongodb://{:}".format(host))
    db = client[database]
    print("MongoDB connection is initiated")
    return client, db

