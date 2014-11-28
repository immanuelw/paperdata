#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass
import os

### Script to load backup of paperdata
### Loads csv file into paperdata to restore deleted table

### Author: Immanuel Washington
### Date: 8-20-14

#User input information
usrnm = raw_input('Username: ')
pswd = getpass.getpass('Password: ')

#searches for only particular files
backup = raw_input('Insert backup manually or automatically? (m/a): ')
if backup == 'm':
	dbnum = raw_input('Insert path of backup: ')
elif backup == 'a':
	dbnum = '/data2/home/immwa/scripts/paperdata/backups/version6_11-27-2014.csv'

#Load data into named database and table

# open a database connection
# be sure to change the host IP address, username, password and database name to match your own
connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)

# prepare a cursor object using cursor() method
cursor = connection.cursor()

print dbnum 
# execute the SQL query using execute() method.
cursor.execute('''LOAD DATA LOCAL INFILE '%s' INTO TABLE paperdata COLUMNS TERMINATED BY ',' LINES TERMINATED BY '\n' '''%(dbnum))

print 'Table data loaded.'

# Close database and save changes
cursor.close()
connection.commit()
connection.close()

# exit the program
sys.exit()
