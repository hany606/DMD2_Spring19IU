# SourcCodes

(0) Open restore.sql and modify the paths of .dat files

(1) Execute ``` sudo -u postgres psql -U postgres -d dvdrental -f <YOUR_PATH>/restore.sql ```

(2) Execute ```mongod```

(3) Execute ```python3 migration.py```

(4) Open YOUR_SETTINGS.json and change it

(5) Execute ```python3 main.py```


### Some problems appeared:

- https://stackoverflow.com/questions/19463074/postgres-error-could-not-open-file-for-reading-permission-denied

- https://mkyong.com/mongodb/mongodb-failed-to-unlink-socket-file-tmpmongodb-27017/ 

- https://stackoverflow.com/questions/7948789/mongod-complains-that-there-is-no-data-db-folder
