#!/usr/bin/python
# -*- coding: utf-8 -*-
# Update data in MySQL table 

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass
import os

### Script to check for all day data compression
### changes ready_to_tape field if all compressed

### Author: Immanuel Washington
### Date: 8-20-14

usrnm = raw_input('Root username: ')
pswd = getpass.getpass('Root password: ')

era = raw_input('32, 64, or 128?: ')
era = int(era)

res = {}

#checks if files of the same Julian Date have all completed compression

# open a database connection
# be sure to change the host IP address, username, password and database name to match your own
connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)

# prepare a cursor object using cursor() method
cursor = connection.cursor()

#counting the amount of files in each day
cursor.execute('SELECT julian_day, COUNT(*) FROM paperdata WHERE era = %d GROUP BY julian_day'%(era))
sec_results = cursor.fetchall()

#counting the amount of compressed files in each day
cursor.execute('''SELECT julian_day, COUNT(*), tape_location FROM paperdata WHERE era = %d and compressed = 1 and raw_location != 'NULL' GROUP BY julian_day'''%(era))
thr_results = cursor.fetchall()

#create dictionary with julian_day as key, count as value
for item in sec_results:
	res.update({item[0]:item[1]})

#testing if same amount in each day, updating if so
for item in thr_results:
	j_value = item[0]
	if item[2] == 'NULL':
		if res[item[0]] == item[1]:
			ready_to_tape = 1
			cursor.execute('''
	                UPDATE paperdata
	                SET ready_to_tape = %d
	                WHERE julian_day = %d;
	                '''%(ready_to_tape, j_value))

print 'Table data updated.'

# close the cursor object
cursor.close()

#save changes to database
connection.commit()

# close the connection
connection.close()

# exit the program
sys.exit()
