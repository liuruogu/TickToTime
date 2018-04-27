# MysqlDataConversion
MysqlDataConversion is to Extract data from power2U webport mysql database and convert date format from 'tick' to readable time format. 

## Functionality

MySQLdb is used to connect to mysql database. It allows python code to use SQL to interact with mysql database. Csv lib is used for read/write csv files. Numpy and pandas is used for processing the loaded data. Datetime is used to convert the 'tick' format to readable time format.

## Useful commands

[How to install Python MySQLdb module using pip?](https://stackoverflow.com/questions/25865270/how-to-install-python-mysqldb-module-using-pip)

    pip install mysqlclient

Please remember to specify the username, password, database in the code.

    db = MySQLdb.connect(host="localhost", user="root", passwd="", db="")

Selected database table is hard coded inthe code, modify them if you need data from other table.

    if ("ems_values" in row[0] or "energymeters" in row[0]):

