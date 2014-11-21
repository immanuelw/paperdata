#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass
import os
import csv
import time
import load_paperdata

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
			return "%3.1f" % (num)
		num /= 1024.0
	num *= 1024.0
	return "%3.1f" % (num)

#User input information
usrnm = raw_input('Username: ')
pswd = getpass.getpass('Password: ')

auto_load = raw_input('Automatically load into paperdata? (y/n): ')

time_date = time.strftime("%d-%m-%Y_%H:%M:%S")
dbnum = '/data4/paper/paperdistiller_output/paperdistiller_output_%s.csv'%(time_date)
dbe = '/data4/paper/paperdistiller_output/paperdistiller_error_%s.csv'%(time_date)

host = socket.gethostname()

#pulls all relevant information from full paperdistiller database
connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdistiller', local_infile=True)

# prepare a cursor object using cursor() method
cursor = connection.cursor()

# execute the SQL query using execute() method.
cursor.execute('SELECT filename, obsnum, md5sum from file order by obsnum')
result = cursor.fetchall()

#gather all data to input into paperdata
results = []
for item in result:
	cursor.execute('SELECT julian_date, pol, length from observation where obsnum = %d'%(item[1])
	sec_results = cursor.fetchall()
	results.append(item + sec_results[0])	

#Create list of obsnums to check for duplicates
cursor.execute('SELECT obsnum from paperdata')
obs = cursor.fetchall()

#Close db connection
cursor.close()
connection.close()

#results list of lists should contain path, obsnum, julian_date, polarization string, and length
for item in results:
	#Opens error logging file
	error_file = open(dbe,'a')
	ewr = csv.writer(error_file, dialect='excel')
	#check for duplicate
	if item[1] in obs:
		err = [item, 'Obsnum already in paperdata']
		ewr.writerow(err)
		error_file.close()
		continue

	#Open file to write to
	data_file = open(dbnum,'a')
	#create 'writer' object
	wr = csv.writer(data_file, dialect='excel')

	#indicates location of raw file (usually same directory as compressed)	
	###need to include host name in path_raw and path
	
	if item[0].split('.')[-1] == 'uv':
		raw_path = item[0]
		raw_full_path = host + ':' + raw_path
		compr_path = item[0] + 'cRRE'
		compr_full_path = host + ':' + compr_path
	elif item[0].split('.')[-1] == 'uv/':
		raw_path = item[0][:-1]
		raw_full_path = host + ':' + raw_path
		compr_path = item[0][:-1] + 'cRRE'
		compr_full_path = host + ':' + compr_path
	else:
		err = [item, 'Not .uv file']
		ewr.writerow(err)
		error_file.close()
		data_file.close()
		continue

	#indicates size of compressed file MB
	compr_file = os.path.join(compr_path, 'visdata')
	if os.path.isfile(compr_file):
		compr_sz = round(sizeof_fmt(get_size(compr_path)), 1)
		compressed = 1
	else:
		compr_sz = 0.0
		compressed = 0
		err = [item, 'No compressed file']
		ewr.writerow(err)
		error_file.close()
		continue
		

	#indicates size of raw file in MB
	raw_file = os.path.join(raw_path, 'visdata')
	if os.path.isfile(raw_file):
		raw_sz = sizeof_fmt(get_size(raw_path))
	else:
		err = [item, 'No .uv file']
		ewr.writerow(err)
		error_file.close()

        #allows uv access
	try:
	        uv = A.miriad.UV(raw_path)
	except:
		err = [item, 'Cannot access .uv file']
		ewr.writerow(err)
		error_file.close()
		data_file.close()
		continue	

	#indicates julian date
	jdate = round(float(item[3]), 5)

	#indicates julian day
	jday = int(str(jdate)[3:7])	

	#indicates set of data used
	if jdate < 2456100:
		era = 32
	elif jdate < 2456400:
		era = 64
	else:
		era = 128

	#indicates type of file in era
	era_type = 'NULL'

	#assign letters to each polarization
	polarization = item[4]

	#indicates length of information in file
	length = round(float(item[5]),5)

	#indicates obsnum
	obsnum = int(item[1])

	#indicates md5sum
	md5sum = item[2]

	#location of calibrate files
	cal_location = 'NULL'

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
	databs = [compr_full_path,era,era_type,obsnum,md5sum,jday,jdate,polarization,length,raw_full_path,cal_location,tape_location,compr_sz,raw_sz,compressed,edge,ready_to_tape,delete_file,restore_history]
	print databs 

	#write to csv file by item in list
	wr.writerow(databs)
	data_file.close()

if auto_load == 'y':
	#Load information into paperdata
	load_paperdata.load_db(dbnum, usrnm, pswd)
else:
	print '''Information logged into '%s' ''' %(dbnum)
