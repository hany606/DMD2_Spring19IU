import psycopg2
from psycopg2 import Error
import json

def get_settings(file_name="settings.json"):
    with open(file_name) as f:
        settings = json.load(f)
    return settings

def init_postgresdb(user, password, host, port, database):
    connection = psycopg2.connect( user = user,
                                    password = password,
                                    host = host,
                                    port = port,
                                    database = database)

    cursor = connection.cursor()
    print("PostgreSQL connection is initiated")
    return connection, cursor

def execute_query(cursor, query):
    cursor.execute(query)
    l = cursor.fetchall()
    return l


def get_cost(res):
    s = res[0][0]
    return((s[s.find("cost")+5:s.find("rows")-1]))

settings = get_settings()
connection, cursor = init_postgresdb(settings["user"], settings["password"], settings["host"], settings["port"], settings["database"])

Query1 =    '''
            EXPLAIN ANALYZE
            SELECT r1.staff_id, p1.payment_date
            FROM rental r1, payment p1
            WHERE r1.rental_id = p1.rental_id AND
            NOT EXISTS (SELECT 1 FROM rental r2, customer c WHERE r2.customer_id = c.customer_id and active = 1 and r2.last_update > r1.last_update);
            '''

Query2 =    '''
            EXPLAIN ANALYZE
            SELECT title, release_year
            FROM film f1
            WHERE f1.rental_rate > (SELECT AVG(f2.rental_rate) FROM film f2 WHERE f1.release_year = f2.release_year);
            '''

Query3 =    '''
            EXPLAIN ANALYZE
            SELECT f.title, f.release_year, (SELECT SUM(p.amount) FROM payment p, rental r1, inventory i1 WHERE p.rental_id = r1.rental_id AND r1.inventory_id = i1.inventory_id AND i1.film_id = f.film_id)
            FROM film f
            WHERE NOT EXISTS (SELECT c.first_name, count(*) FROM customer c, rental r2, inventory i1, film f1, film_actor fa, actor a 
            WHERE c.customer_id = r2.customer_id AND r2.inventory_id = i1.inventory_id AND i1.film_id = f1.film_id and f1.rating in ('PG-13','NC-17') AND f1.film_id = fa.film_id AND f1.film_id = f.film_id AND fa.actor_id = a.actor_id and a.first_name = c.first_name GROUP BY c.first_name HAVING count(*) >2);
            '''

print("Cost = {:}".format(get_cost(execute_query(cursor, Query1))))
print("Cost = {:}".format(get_cost(execute_query(cursor, Query2))))
print("Cost = {:}".format(get_cost(execute_query(cursor, Query3))))
