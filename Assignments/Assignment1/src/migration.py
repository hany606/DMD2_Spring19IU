import psycopg2
from psycopg2 import Error

from pymongo import MongoClient
import pymongo

from bson.decimal128 import Decimal128
from bson import decode_all
from bson import json_util

import datetime

import decimal

user = "postgres"
password = "hany123456"
host = "127.0.0.1"
port = "5432"
database = "dvdrental"

def init_postgresdb():
    connection = psycopg2.connect( user = user,
                                    password = password,
                                    host = host,
                                    port = port,
                                    database = database)

    cursor = connection.cursor()
    print("PostgreSQL connection is initiated")
    return connection, cursor

def init_mongodb():
    client = MongoClient("mongodb://{:}".format(host))
    db = client[database]
    print("MongoDB connection is initiated")
    return client, db

def getTables_psgdb(cursor):
    # Source: https://stackoverflow.com/questions/10598002/how-do-i-get-tables-in-postgres-using-psycopg2
    cursor.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
    l = cursor.fetchall()
    return l

def getRecords_psgdb(cursor, table_name):
    cursor.execute("select * from {:}".format(table_name))
    rec = cursor.fetchall()
    # Source: https://stackoverflow.com/questions/10252247/how-do-i-get-a-list-of-column-names-from-a-psycopg2-cursor
    col = [desc[0] for desc in cursor.description]
    return rec, col

def createCollection_mngdb(db, table_name):
    collection = db["test"]
    return collection

def createRecord_mngdb(collection, column, data):
    record = {}
    for i,c in enumerate(column):
        if(type(data[i]) is datetime.date):
            record[c] = datetime.datetime(data[i].year, data[i].month, data[i].day, 0, 0)
            continue
        if(type(data[i]) is memoryview):
            # print("--------------------Fixed#memoryview------------")
            # record[c] = decode_all(memoryview(data[i]))
            # print(data[i].tobytes())
            # print(data[i])
            record[c] = str(data[i].tobytes())
            # print(record[c])
            continue
        if(type(data[i]) is decimal.Decimal):
            # print("--------------------Fixed#Decimal------------")
            record[c] = Decimal128(data[i])
            continue
        record[c] = data[i]
    print(record)
    inserted_id = collection.insert_one(record).inserted_id
    return inserted_id


connection_pg, cursor_pg = init_postgresdb()

client_mngdb, db_mngdb = init_mongodb()

# get all the tables names in postgres sql
tables = getTables_psgdb(cursor_pg)

for t in tables:
    collection = createCollection_mngdb(db_mngdb, t[0])
    records, columns = getRecords_psgdb(cursor_pg, t[0])
    for r in records:
        inserted_id = createRecord_mngdb(collection, columns, r)

        # print(columns)
        # print(r)
        # break
    # break


connection_pg.close()
cursor_pg.close()

client_mngdb.close()