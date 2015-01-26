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

### Script to load data from anywhere into paperfeed database
### Crawls folio or elsewhere and reads through .uv files to generate all field information

### Author: Immanuel Washington
### Date: 11-23-14

def load_db(dbo, usrnm, pswd):
	#Load data into named database and table
	# open a database connection
	connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)

	# prepare a cursor object using cursor() method
	cursor = connection.cursor()

	#execute the SQL query using execute() method.
	cursor.execute('''LOAD DATA LOCAL INFILE %s INTO TABLE paperfeed COLUMNS TERMINATED BY '|' LINES TERMINATED BY '\n' ''', (dbo,))

	print 'Table data loaded.'

	#Close and save changes to database
	cursor.close()
	connection.commit()
	connection.close()

	return None

def gen_paperfeed(dirs, dbo, dbe):
	host = 'folio'

	#Erase former data file
	data_file = open(dbo,'wb')
	data_file.close()

	full_info = []

	for dir in dirs[:]:

		#create csv file to log data
		data_file = open(dbo,'ab')
		wr = csv.writer(data_file, delimiter='|', lineterminator='\n', dialect='excel')

		#create csv file to log bad files
		error_file = open(dbe, 'ab')
		ewr = csv.writer(error_file, delimiter='|', lineterminator='\n', dialect='excel')

		#checks if file loaded in is raw or compressed - makes changes to compensate
		if dir.split('.')[-1] == 'uv':
			print dir
			#indicates name of full directory -- SHOULD I SET TO NULL? OR CHECK DATABASE EVERY TIME?
			raw_full_path = host + ':' + dir
			raw_path = dir
			path = dir
		else:
			continue

		#checks to make sure file can be accessed

		#temp fix
		if dir == '/nas2/data/psa6668/zen.2456668.17386.yx.uvcRRE':
			item = [path,'Unknown error']
			ewr.writerow(item)
			error_file.close()
			continue

		#checks a .uv file for data
		visdata = os.path.join(path, 'visdata')
		if not os.path.isfile(visdata):
			item = [path,'No visdata']
			ewr.writerow(item)
			error_file.close()
			continue

		#checks a .uv file for vartable
		vartable = os.path.join(path, 'vartable')
		if not os.path.isfile(vartable):
			item = [path,'No vartable']
			ewr.writerow(item)
			error_file.close()
			continue

		#checks a .uv file for header
		header = os.path.join(path, 'header')
		if not os.path.isfile(header):
			item = [path,'No header']
			ewr.writerow(item)
			error_file.close()
			continue

		#allows uv access
		try:
			uv = A.miriad.UV(path)
		except:
			item = [path,'Cannot access .uv file']
			ewr.writerow(item)
			error_file.close()
			continue	

		#indicates julian date
		jdate = round(uv['time'], 5)

		jday = int(str(jdate)[3:7])

		#moved defaults to 0
		moved = 0

		#create list of important data and open csv file
		databs = [raw_full_path, jday, moved]
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

	cursor.execute('''SELECT raw_path from paperfeed''')
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
	usrnm = raw_input('Username: ')
	pswd = getpass.getpass('Password: ')

	datanum = raw_input('Input file path: ')

	dbo = '/data4/paper/paperoutput/paperfeed_out.psv'
	dbe = '/data4/paper/paperoutput/false_paperfeed.psv'

	#iterates through directories, listing information about each one
	dirs_all = glob.glob(datanum)

	#removes duplicate entries from directory
	dirs = remove_duplicates(dirs_all, usrnm, pswd)

	auto_update = raw_input('Auto-load immediately after finishing (y/n)?: ')

	dirs.sort()
	gen_paperfeed(dirs, dbo, dbe)

	if auto_update == 'y':
		usrnm2 = raw_input('Input username with edit privileges: ')
		pswd2 = getpass.getpass('Input password: ')
		load_db(dbo, usrnm2, pswd2)
		sys.exit()
