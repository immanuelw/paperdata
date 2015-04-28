#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass
import os
import csv
import glob
import socket
import aipy as A
import paperdataDB as pdb

### Script to load data from pot0 into paperjunk
### Crawls pot0 and saves all file paths to table paperjunk

### Author: Immanuel Washington
### Date: 11-23-14

def load_db_from_file(dbo, table, usrnm, pswd):
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

def gen_paperjunk(dirs, dbo):
	host = 'folio'

	#Erase former data file
	data_file = open(dbo,'wb')
	data_file.close()

	full_info = []

	for dir in dirs[:]:

		#create csv file to log data
		data_file = open(dbo,'ab')
		wr = csv.writer(data_file, delimiter='|', lineterminator='\n', dialect='excel')

		junk_path = host + ':' + dir
		folio_path = 'NULL'
		junk_size = os.path.getsize(dir)
		USB = int(dir.split('/')[3])
		renamed = 0

		databs = (junk_path,
					folio_path,
					junk_size,
					USB,
					renamed)
		print databs

		#write to csv file
		wr.writerow(databs)
		full_info.append(databs)

		#save into file and close it
		data_file.close()

	return full_info

def remove_duplicates(dirs_all, usrnm, pswd):
	#Removes all files from list that already exist in the database
	connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)

	cursor = connection.cursor()

	cursor.execute('''SELECT junk_path from paperjunk''')
	results = cursor.fetchall()
	cursor.close()
	connection.close()

	for res in results:
		if res[0] != 'NULL':
			folderC = res[0].split(':')[1]
		else:
			folderC = 'NULL'

		try:
			dirs_all.remove(folderC)
		except:
			continue

	return dirs_all

if __name__ == '__main__':

	#User input information
	usrnm = 'paperboy'
	pswd = 'paperboy'

	datanum = raw_input('Input file path: ')

	dbo = '/data4/paper/paperoutput/paperjunk_out.psv'

	#iterates through directories, listing information about each one
	dirs_all = glob.glob(datanum)

	#removes duplicate entries from directory
	dirs = remove_duplicates(dirs_all, usrnm, pswd)

	auto_update = raw_input('Auto-load immediately after finishing (y/n)?: ')

	dirs.sort()
	gen_paperjunk(dirs, dbo)

	if auto_update == 'y':
		usrnm2 = raw_input('Input username with edit privileges: ')
		pswd2 = raw_input('Input password: ')
		table = 'paperjunk'
		load_db_from_file(dbo, table, usrnm2, pswd2)
		sys.exit()
