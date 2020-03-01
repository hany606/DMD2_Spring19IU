# This exercises have been written during a lab which it duration was 1.5 hour and we have finished early in 1 hour
from pymongo import MongoClient
from datetime import datetime

client = MongoClient("mongodb://localhost")

db = client["test"]

print(" DB is connectd ")

def return_cursor(config):
	return db.restaurants.find(config)

def print_cursor(cursor, comment=None):
	sep_string = "--------------{:}--------------".format(comment if comment is not None else "")
	print(sep_string)	
	for i in cursor:
		print(i)
	print(sep_string)


def ex1():
	print("---------------EX1-------------")
	cursor = return_cursor({"cuisine": "Indian"})
	print_cursor(cursor)

	cursor = return_cursor({"$or": [{"cuisine": "Indian"}, {"cuisine": "Thai"}] })
	print_cursor(cursor)

	cursor = return_cursor({"address.building": "1115", "address.street": "Rogers Avenue", "address.zipcode": "11226"})
	print_cursor(cursor)

	cursor = return_cursor({"borough": "Manhatten"})
	print_cursor(cursor)

def ex2():
	print("---------------EX2-------------")
	db.restaurants.insert_one({"address":{"building": "1480", "street": "2 Avenue", "zipcode": "10075", "coord":[ -73.9557413, 40.7720266]}, "borough": "Manhatten", "cuisine": "Italian", "name": "Vella", "restaurant_id": 41704620, "grade": {"grade": "A", "score": 11, "date": datetime(2014,10,1)}})
	
	
def ex3():
	print("---------------EX3-------------")
	print("Before deletion")
	cursor = return_cursor({"borough": "Manhatten"})
	print_cursor(cursor)

	db.restaurants.delete_one({"borough": "Manhatten"})
	print("After deletion")
	cursor = return_cursor({"borough": "Manhatten"})
	print_cursor(cursor)


	print("Before deletion")
	cursor = return_cursor({"cuisine": "Thai"})
	print_cursor(cursor)

	db.restaurants.delete_many({"cuisine": "Thai"})
	print("After deletion")
	cursor = return_cursor({"cuisine": "Thai"})
	print_cursor(cursor)


def ex4():
	print("Before")
	cursor = return_cursor({"address.street": "Rogers Avenue"})
	for i in cursor:
		print(i)
		break
	for i in cursor:
		count = 0
		for x in i["grades"]:
			count += 1 if x["grade"] == "C" else 0
			if(count >= 2):
				break
		if(count >= 2):
			db.restaurants.delete_one(i)
		else:
			list_grades = i["grades"]
			list_grades.append({"grade":"C"})
			db.restaurants.update_one(i, {"$set": { "grades": list_grades} })
	
	print("After")
	cursor = return_cursor({"address.street": "Rogers Avenue"})
	for i in cursor:
		print(i)
		break

#ex1()

#ex2()

#ex3()

ex4()
