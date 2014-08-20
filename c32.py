#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass
import os
import inspect
import csv

#Functions which simply find the file size of the .uvcRRE files
def get_size(start_path):
	total_size = 0
	for dirpath, dirnames, filenames in os.walk(start_path):
		for f in filenames:
			fp = os.path.join(dirpath, f)
			total_size += os.path.getsize(fp)
	return total_size

def sizeof_fmt(num):
	for x in ['bytes','KB','MB','GB']:
		if num < 1024.0:
			return "%3.1f%s" % (num, x)
		num /= 1024.0
	return "%3.1f%s" % (num, 'TB')

#User input information
datab = 'paperdata'
usrnm = raw_input('Username: ')
pswd = getpass.getpass('Password: ')

#combined all eras into one table
table_name = 'paperdata'

# open a database connection
# be sure to change the host IP address, username, password and database name to match your own
connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)

# prepare a cursor object using cursor() method
cursor = connection.cursor()

cursor.execute('SELECT path, obsnum FROM paperdata WHERE era = 32')
results = cursor.fetchall()

for item in results:
	path = item[0]
	obsnum = item[1]
	raw_loc = path[:-4]
	print path
	print raw_loc
	if os.path.isdir(raw_loc):
		# execute the SQL query using execute() method.
		cursor.execute('''
		UPDATE paperdata
		SET raw_location = '%s'
		WHERE obsnum = %d
		'''%(raw_loc, obsnum))

print 'Table data loaded.'

# close the cursor object
cursor.close()

#save changes to database
connection.commit()

# close the connection
connection.close()

# exit the program
sys.exit()
