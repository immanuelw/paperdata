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
import paperdataDB as pdb

### Script to load backup of paperdata
### Loads csv file into paperdata to restore deleted table

### Author: Immanuel Washington
### Date: 8-20-14

def load_backup_from_file(dbo, table, usrnm, pswd):
	#Load data into named database and table
	# open a database connection
	connection = MySQLdb.connect (host = 'shredder', user = usrnm, password = pswd, database = 'paperdata')

	# prepare a cursor object using cursor() method
	cursor = connection.cursor()

	outfile = dbo %(table)

	#execute the SQL query using execute() method.
	insert_base = '''INSERT INTO %s VALUES(%s)'''

	if table not in pdb.classes:
		print 'Incorrect table name'
		return None
	var_class = pdb.instant_class[table]
	table_vars = (var_class.table, var_class.values)

	#load information from file into eatwell database
	with open(outfile, 'rb') as read_file:
		read = csv.reader(read_file, delimiter='|', lineterminator='\n', dialect='excel')
		for row in read:
			val = tuple(row)
			insert = insert_base % table_vars
			values = val
			cursor.execute(insert, values)

	print 'Table data loaded.'

	#Close and save changes to database
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
	table = 'paperdata'

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
			dbnum = '/data2/home/immwa/scripts/paperdata/backups/version11_12-20-2014.psv'
		load_backup_from_file(dbnum, table, usrnm, pswd)
