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
import time
import socket
import filecmp

### Script to load infromation quickly from paperdistiller database into paperdata
### Queries paperdistiller for relevant information, loads paperdata with complete info

### Author: Immanuel Washington
### Date: 8-20-14

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

paperd = 'paperdistiller'

time_date = time.strftime("%d-%m-%Y_%H:%M:%S")
db128 = '/data2/home/immwa/scripts/paper_output/db_output128_%s.csv'%(time_date)

#check if paperdistiller has already been crawled
crawl = raw_input('Check paperdistiller for deletion (d) or loading (l) ?: ')
if crawl == 'd':
	backup = raw_input('Insert path of last backup of paperdistiller: ')

host = socket.gethostname()

#searches for only particular files
dbnum = db128

#combined all eras into one table
table_name = 'paperdata'

resultFile = open(dbnum,'wb')

#create 'writer' object
wr = csv.writer(resultFile, dialect='excel')

#pulls all relevant information from full paperdistiller database

connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdistiller', local_infile=True)

# prepare a cursor object using cursor() method
cursor = connection.cursor()

# execute the SQL query using execute() method.
cursor.execute('SELECT filename, obsnum from file order by obsnum')

results = cursor.fetchall()

for item in results:
	cursor.execute('SELECT julian_date, pol, length from observation where obsnum = %d'%(item[1])

sec_results = cursor.fetchall()
# close the cursor object
cursor.close()

#save changes to database
connection.commit()

# close the connection
connection.close()


#gather all the the data to input into paperdata

for item in results:
	item += sec_results[0]
	item += sec_results[1]
	item += sec_results[2]

print item

#results list of lists should contain path, obsnum, julian_date, polarization string, and length

for item in results:
	#indicates location of raw file (usually same directory as compressed)	
	path_raw = results[0]
	
	path = results[0] + 'cRRE'

	#indicates size of file
	sz = sizeof_fmt(get_size(path))

	#checks if any data is in file
	visdata = os.path.join(path_raw, 'visdata')
	if not os.path.isfile(visdata):
		continue

        #allows uv access
	try:
	        uv = A.miriad.UV(path)
	except:
		continue	

	#indicates julian date
	jdate = results[2]

	#indicates julian day
	jday = int(str(jdate)[3:7])	

	#indicates set of data used
	era = 128

	#indicates type of file in era
	era_type = 'NULL'

	#assign letters to each polarization
	polarization = results[3]

	#indicates length of information in file
	length = results[4]

	#indicates obsnum
	obsnum = results[1]

	#location of raw files
	raw_location = path_raw #do not know where they are for any of them yet

	#location of calibrate files
	cal_location = 'NULL'

	#indicates if file is compressed
	if os.path.isfile(path):
		compressed = 1
	else:
		compressed = 0

	#shows location of raw data on tape
	tape_location = 'NULL'

	#variable indicating if all files have been successfully compressed in one day
	ready_to_tape = 0

	#indicates if all raw data is compressed, moved to tape, and the raw data can be deleted from folio
	delete_file = 0 

	#create list of important data and open csv file
	databs = [[host,path,era,era_type,obsnum,jday,jdate,polarization,length,raw_location,cal_location,tape_location,str(sz),compressed,ready_to_tape,delete_file]]
	print databs 

	#write to csv file by item in list
	for item in databs:
		wr.writerow(item)

#Don't load if paperdistiller can be deleted
if crawl == 'd':
	if filecmp.cmp(db128,backup):
		sys.exit()
	else:
		print 'backups have different information'
		sys.exit()

#Load data into named database and table

# open a database connection
# be sure to change the host IP address, username, password and database name to match your own
connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)

# prepare a cursor object using cursor() method
cursor = connection.cursor()

print dbnum 
# execute the SQL query using execute() method.
cursor.execute('''USE paperdata;
LOAD DATA LOCAL INFILE '%s' INTO TABLE paperdata
COLUMNS TERMINATED BY ','
LINES TERMINATED BY '\n' '''%(dbnum))

print 'Table data loaded.'

# close the cursor object
cursor.close()

#save changes to database
connection.commit()

# close the connection
connection.close()

# exit the program
sys.exit()
