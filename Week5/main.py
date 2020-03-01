from DB import Hospital
import Querries

db = Hospital(database="Hospital")
db.generate()
db.pg_query("schema.sql")

# db.pg_query("data/dep_lab.sql")
# db.pg_query("data/person_member.sql")
# db.pg_query("data/contact_details.sql")
# db.pg_query("data/appointment.sql")
# db.pg_query("data/complaint.sql")
# db.pg_query("data/medicine_surgery.sql")
# db.pg_query("data/events_other.sql")
# db.pg_query("data/feedback.sql")
# db.pg_query("data/schedule.sql")  
# db.pg_query("data/appointment_q3_sick.sql")


print("Commands:\n\
      0 --> Exit\n\
      1 -- 5 --> Query num. \n\
      c --> Clear and generate new dataset")

# while(True):
time_taken_avg = 0
for i in range(1000):
    time_taken_avg += Querries.Dummy.execute()
    
print("Time in avg taken to do this query: {:}".format(time_taken_avg/1000))