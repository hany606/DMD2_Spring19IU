import psycopg2
import time

class Dummy:
    @staticmethod
    def execute():

        con = psycopg2.connect(database="Hospital", user="postgres", password="123456789", host="127.0.0.1",
                               port="5432")
        cur = con.cursor()
        time1 = time.time()
        cur.execute("SELECT * FROM person")

        rows = cur.fetchall()
        time_taken = time.time() - time1
        # print("time_take: {:}".format(time_taken))
        # print(len(rows))
        if(len(rows) == 0):
            print("No data to be shown")
        return time_taken
        # else:
            # print(rows)