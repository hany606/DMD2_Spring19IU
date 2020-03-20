# ---------------------------------------------------------------------------------------------------
# Author: Hany Hamed
# Description: Source file for wrapping all the queries codes [for Assignment 1 for DMD2 course]
# ---------------------------------------------------------------------------------------------------

from utils import *
from datetime import datetime
import pprint
from query1 import Query1
from query2 import Query2
from query3 import Query3
from query4 import Query4
from query5 import Query5

class Queries:
    def __init__(self,db):
        self.db = db
        self.pp = pprint.PrettyPrinter()

    def query(self, q_num, params=None):
        queries = [Query1, Query2, Query3, Query4, Query5]
        queries[q_num-1](self.db).execute(params)