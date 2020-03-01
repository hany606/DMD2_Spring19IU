import psycopg2


connection = psycopg2.connect( user = 'postgres',
                                    password = "123456789",
                                    host = "127.0.0.1",
                                    port = "5432",
                                    database = "Hospital")

cursor = connection.cursor()


query = """
        Create Table customer(
            id SERIAL PRIMARY KEY,
            name VARCHAR(50),
            address VARCHAR(255),
            review VARCHAR(255)
        );
        """

cursor.execute(query)



def populate(info):
   return ("INSERT INTO Customer(id, name, address, review) Values({:}, {:}, {:}, {:});".format(info["id"], info["name"], info["address"], info["review"])) 


ideal_info = {"name": "Good blablabla", "address": "innopolis", "review": "Good review"}
# for i in range(10000):
#     info = {}
#     info["id"] = i+1
#     if(i%100):
#         info.update(ideal_info)
#     else:
#         info["name"] = "bla"+ "bla"
#         info["address"] = "asdlknawdsnkldlas"
#         info["review"] = "asdmasdlknasmd"
#     # cursor.execute(populate(info))
#     postgres_insert_query = """ INSERT INTO Customer (id, name, address, review) VALUES (%s,%s,%s,%s)"""
#     record_to_insert = (i+1, info["name"], info["address"], info["review"])
#     cursor.execute(postgres_insert_query, record_to_insert)
#     print("#{:}th finished".format(i+1))


cursor.close()
connection.close()