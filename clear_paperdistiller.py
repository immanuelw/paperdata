#!/usr/bin/python
import sys
import MySQLdb

### Script to clear paperdistiller database
### Empties paperdistiller of all entries in every table

### Author: Immanuel Washington
### Date: 8-20-14

#login info for paperdistiller database
usrnm = raw_input('Root User: ')
pswd = getpass.getpass('Root Password: ')

# open a database connection
connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdistiller', local_infile=True)

cursor = connection.cursor()

# execute the SQL query using execute() method -- clears database by truncating each table
cursor.execute('set foreign_key_checks = 0; TRUNCATE neighbors; TRUNCATE observation; TRUNCATE file, TRUNCATE log; set foreign_key_checks = 1')

#close, save and end connection
cursor.close()
connection.commit()
connection.close()

sys.exit()
