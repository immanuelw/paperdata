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
	for x in ['bytes','KB','MB']:
		if num < 1024.0:
			return "%3.1f%s" % (num, x)
		num /= 1024.0
	num *= 1024.0
	return "%3.1f" % (num)

#User input information
datab = 'paperdata'
usrnm = raw_input('Username: ')
pswd = getpass.getpass('Password: ')

paperd = 'paperdistiller'

time_date = time.strftime("%d-%m-%Y_%H:%M:%S")
db128 = '/data2/home/immwa/scripts/paper_output/db_output128_%s.csv'%(time_date)

host = socket.gethostname()

#searches for only particular files
dbnum = db128

resultFile = open(dbnum,'wb')

#create 'writer' object
wr = csv.writer(resultFile, dialect='excel')

#empty list to use later
results = []

#pulls all relevant information from full paperdistiller database

connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdistiller', local_infile=True)

# prepare a cursor object using cursor() method
cursor = connection.cursor()

# execute the SQL query using execute() method.
cursor.execute('SELECT filename, obsnum, md5sum from file order by obsnum')

result = cursor.fetchall()

#gather all data to input into paperdata
for item in result:
	cursor.execute('SELECT julian_date, pol, length from observation where obsnum = %d'%(item[1])
	sec_results = cursor.fetchall()
	result.append(item + sec_results)	

#Close database connection and save changes
cursor.close()
connection.close()

#Create list of obsnums to check for duplicates
connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)

cursor = connection.cursor()

cursor.execute('SELECT obsnum from paperdata')
obs = cursor.fetchall()

#Close db connection and save changes
cursor.close()
connection.close()

#results list of lists should contain path, obsnum, julian_date, polarization string, and length
for item in results:
	#check for duplicate
	if item[1] in obs:
		continue

	#indicates location of raw file (usually same directory as compressed)	
	###need to include host name in path_raw and path
	path_raw = item[0]
	
	if item[0].split('.')[-1] == 'uv':
		path = item[0] + 'cRRE'
		compr_path = host + ':' + item[0] + 'cRRE'
	elif item[0].split('.')[-1] == 'uv/':
		path = item[0][:-1] + 'cRRE'
		compr_path = host + ':' + item[0][:-1] + 'cRRE'

	#indicates size of compressed file MB
	sz = sizeof_fmt(get_size(path))

	#indicates size of raw file in MB
	raw_sz = sizeof_fmt(get_size(path_raw))

	#checks if any data is in file
	visdata = os.path.join(path_raw, 'visdata')
	if not os.path.isfile(visdata):
		continue

        #allows uv access
	try:
	        uv = A.miriad.UV(path_raw)
	except:
		continue	

	#indicates julian date
	jdate = item[3]

	#indicates julian day
	jday = int(str(jdate)[3:7])	

	#indicates set of data used
	era = 128

	#indicates type of file in era
	era_type = 'NULL'

	#assign letters to each polarization
	polarization = item[4]

	#indicates length of information in file
	length = item[5]

	#indicates obsnum
	obsnum = item[1]

	#indicates md5sum
	md5sum = item[2]

	#location of raw files
	raw_location = host + ':' + path_raw #do not know where they are for any of them yet

	#location of calibrate files
	cal_location = 'NULL'

	#indicates if file is compressed
	compr_file = os.path.join(path,'visdata')
	if os.path.isfile(compr_file):
		compressed = 1
	else:
		compressed = 0

	#shows location of raw data on tape
	tape_location = 'NULL'

	#Show if file is edge file
	edge = 0

	#variable indicating if all files have been successfully compressed in one day
	ready_to_tape = 0

	#indicates if all raw data is compressed, moved to tape, and the raw data can be deleted from folio
	delete_file = 0 

	#indicates when a file has been restored
	restore_history = 'NULL'

	#create list of important data and open csv file
	databs = [compr_path,era,era_type,obsnum,md5sum,jday,jdate,polarization,length,raw_location,cal_location,tape_location,sz,raw_sz,compressed,edge,ready_to_tape,delete_file,restore_history]
	print [databs] 

	#write to csv file by item in list
	wr.writerow(databs)

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

#Close db connection and save changes
cursor.close()
connection.commit()
connection.close()

# exit the program
sys.exit()
