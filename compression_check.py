#!/usr/bin/python
# -*- coding: utf-8 -*-
# Update data in MySQL table 

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass
import os

### Script to check for all day data compression

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

#set value to compressed files

cursor.execute('SELECT obsnum, path FROM paperdata WHERE era = %d ORDER BY julian_date'%(era))
fir_results = cursor.fetchall()

#check if compressed file exists, if so set compr_value = 1
for item in fir_results:
	obsnum = item[0]
	path = item[1].split(':')[1]
	path_file = os.path.join(path,'visdata')
	if os.path.isfile(path_file):
		compr_value = 1
	else:
		compr_value = 0
	cursor.execute('''
	UPDATE paperdata
	SET compressed = %d
	WHERE obsnum = %d;
	'''%(compr_value, obsnum))

print 'Table data updated.'

# close the cursor object
cursor.close()

#save changes to database
connection.commit()

# close the connection
connection.close()

# exit the program
sys.exit()
