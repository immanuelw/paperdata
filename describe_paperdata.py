#!/usr/bin/python
# -*- coding: utf-8 -*-
# Create MySQL schema file

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass

### Script to generate information about paperdata schema
### Opens MySQL through module, creates file by iterating through list query generates

### Author: Immanuel Washington
### Date: 10-07-14

#inputs for user to access database
pswd = getpass.getpass('Password: ')

#creates file to write to
pd = open('/data2/home/immwa/scripts/paper/paperdata_schema', 'w')

# open a database connection
# be sure to change the host IP address, username, password and database name to match your own
connection = MySQLdb.connect (host = 'shredder', user = 'immwa', passwd = pswd, db = 'paperdata', local_infile=True)

# prepare a cursor object using cursor() method
cursor = connection.cursor()

# execute the SQL query using execute() method.
# Builds table by fields including defaults
cursor.execute('DESC paperdata')

results = cursor.fetchall()

for item in results:
	pd.writeline(item)	

# Close and save all changes to database
cursor.close()
connection.commit()
connection.close()

# exit the program
sys.exit()

