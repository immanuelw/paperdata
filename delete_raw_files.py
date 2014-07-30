#!/usr/bin/python
# -*- coding: utf-8 -*-
# Update data in MySQL table 

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass
import shutil

table = 'paperdata'
usrnm = raw_input('Root username: ')
pswd = getpass.getpass('Root password: ')

raw = 'raw_location'
obsnum_string = 'obsnum'
delt = 'ready_to_delete'

raw_value = 'NULL'
deletion = []

# open a database connection
# be sure to change the host IP address, username, password and database name to match your own
connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)

# prepare a cursor object using cursor() method
cursor = connection.cursor()

#execute MySQL query
cursor.execute('SELECT julian_day, obsnum, raw_location, tape_location, ready_to_delete from paperdata order by julian_day')

#collects information from query
results = cursor.fetchall()

#results is a list of lists
for items in results:
	obsnum = items[1]
	if items[4] == True and not items[3] == 'NULL' and not items[2] == 'NULL':
		deletion.append(items[2])
		del_value = False

		# execute the SQL query using execute() method.
		cursor.execute('''
		UPDATE %s
		SET %s = %s, %s = %s
		WHERE %s = %d;
		'''%(table, delt, del_value, raw, raw_value, obsnum_string, obsnum)) 

#loops through list and deletes raw files scheduled for deletion
for item in deletion:
	shutil.rmtree(item)

print 'Table data updated.'

# close the cursor object
cursor.close()

#save changes to database
connection.commit()

# close the connection
connection.close()

# exit the program
sys.exit()
