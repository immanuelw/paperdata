#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass
import os
import subprocess
import time

### Script to load backup of paperdata
### Loads csv file into paperdata to restore deleted table

### Author: Immanuel Washington
### Date: 8-20-14

def load_backup(dbnum, usrnm, pswd):
	#Load data into named database and table

	# open a database connection
	# be sure to change the host IP address, username, password and database name to match your own
	connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)

	# prepare a cursor object using cursor() method
	cursor = connection.cursor()

	print dbnum 
	# execute the SQL query using execute() method.
	cursor.execute('''LOAD DATA LOCAL INFILE '%s' INTO TABLE paperdata COLUMNS TERMINATED BY '|' LINES TERMINATED BY '\n' '''%(dbnum))

	print 'Table data loaded.'

	# Close database and save changes
	cursor.close()
	connection.commit()
	connection.close()

	return None

def load_sql_backup(dbnum, usrnm, pswd):
	file = open(dbnum, 'rb')
	subprocess.call(['mysql', '-h', 'shredder', '-u', usrnm, '--password=%s'%(pswd), 'paperdata'], stdin=file)
	file.close()

	return None

if __name__ == '__main__':
	#User input information
	usrnm = raw_input('Username: ')
	pswd = getpass.getpass('Password: ')

	#searches for only particular files
	full = raw_input('Reload entire database?(y/n): ')
	backup = raw_input('Insert backup path manually or automatically? (m/a): ')
	if full =='y':
		if backup == 'm':
			dbnum = raw_input('Insert path of backup: ')
		elif backup == 'a':
			dbnum = '/data2/home/immwa/scripts/paperdata/backups/paperdata_02-12-2014_20:33:29.sql'
		load_sql_backup(dbnum, usrnm, pswd)
	else:
		if backup == 'm':
			dbnum = raw_input('Insert path of backup: ')
		elif backup == 'a':
			dbnum = '/data2/home/immwa/scripts/paperdata/backups/version9-1_12-11-2014.psv'
		load_backup(dbnum, usrnm, pswd)
