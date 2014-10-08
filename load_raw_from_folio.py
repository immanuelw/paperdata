#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass
import os
import csv
import aipy as A
import hashlib
import glob
import socket
import decimal

### Script to load data from folio into paperdata database
### Crawls folio and reads through .uvcRRE files to generate all field information

### Author: Immanuel Washington
### Date: 8-20-14

#counting variables
t_min = 0
t_max = 0
n_times = 0
c_time = 0

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
usrnm = raw_input('Username: ')
pswd = getpass.getpass('Password: ')

datanum = raw_input('Input file path: ')

dbo = '/data2/home/immwa/scripts/paper_output/db_out.csv'

host = socket.gethostname()

resultFile = open(dbo,'wb')

#create 'writer' object
wr = csv.writer(resultFile, dialect='excel')

#create csv file to log bad files
error_file = open('/data2/home/immwa/scripts/paper_output/false.csv', 'a')
ewr = csv.writer(error_file, dialect='excel')

#create function to uniquely identify files
def jdpol2obsnum(jd,pol,djd):
	"""
	input: julian date float, pol string. and length of obs in fraction of julian date
	output: a unique index
	"""
	dublinjd = jd - 2415020  #use Dublin Julian Date
	obsint = int(dublinjd/djd)  #divide up by length of obs
	polnum = A.miriad.str2pol[pol]+10
	assert(obsint < 2**31)
	return int(obsint + polnum*(2**32))

def md5sum(fname):
	"""
	calculate the md5 checksum of a file whose filename entry is fname.
	"""
	fname = fname.split(':')[-1]
	BLOCKSIZE = 65536
	hasher = hashlib.md5()
	try:
		afile = open(fname, 'rb')
	except(IOError):
		afile = open("%s/visdata"%fname, 'rb')
	buf = afile.read(BLOCKSIZE)
	while len(buf) >0:
		hasher.update(buf)
		buf = afile.read(BLOCKSIZE)
	return hasher.hexdigest()

decimal.getcontext().prec = 2

#iterates through directories, listing information about each one
dirs = glob.glob(datanum)
for dir in dirs:
	compr_path = 'NULL'
	compr_file_size = 0.0

	#location of raw files
	raw_path = host + ':' + dir
	raw_location = dir
	if not os.path.isdir(raw_location.split(':')[1]):
		raw_location = 'NULL'

	#allows uv access
	try:
		uv = A.miriad.UV(raw_location)
	except:
		error_list = [[path,'Cannot access .uv file']]
		for item in error_list:
			ewr.writerow(item)
		continue	

	#indicates julian date
	jdate = uv['time']

	#indicates julian day and set of data
	if jdate < 2456100:
		jday = int(str(jdate)[4:7])
		era = 32
	else:
		jday = int(str(jdate)[3:7])	
		if jdate < 2456400:
			era = 64
		else:
			era = 128

	#indicates type of file in era
	era_type = 'NULL'

	#assign letters to each polarization
	if uv['npol'] == 1:
		if uv['pol'] == -5:
			polarization = 'xx'
		elif uv['pol'] == -6:
			polarization = 'yy'
		elif uv['pol'] == -7:
			polarization = 'xy'
		elif uv['pol'] == -8:
			polarization = 'yx' 
	elif uv['npol'] == 4:
	#	polarization = 'all' #default to 'yy' as 'all' is not a key for jdpol2obsnum
		polarization = 'yy'

	length = 0

	#gives each file unique id
	obsnum = 0

	#location of raw files
	raw_path = host + ':' + dir
	raw_location = dir
	if not os.path.isdir(raw_location.split(':')[1]):
		raw_location = 'NULL'

	#gives each file more unique id
	mdsum = md5sum(raw_location.split(':')[1])

	#size of raw file, removing unit size
	big_byte = sizeof_fmt(get_size(raw_location.split(':')[1]))

	if big_byte[-1] == 'B':
		raw_file_size = decimal.Decimal(big_byte[:-2])
	elif big_byte[-1] == 's':
		raw_file_size = decimal.Decimal(big_byte[:-5])
	else:
		raw_file_size = decimal.Decimal(big_byte)

	#location of calibrate files
	if era == 32:
		cal_location = '/usr/global/paper/capo/arp/calfiles/psa898_v003.py'
	elif era == 64:
		cal_location = '/usr/global/paper/capo/zsa/calfiles/psa6240_v003.py'
	elif era == 128:
		cal_location = 'NULL'

	#indicates if file is compressed
	compressed = 0

	#shows location of raw data on tape
	tape_location = 'NULL'

	#variable indicating if all files have been successfully compressed in one day
	ready_to_tape = 0

	#indicates if all raw data is compressed, moved to tape, and the raw data can be deleted from folio
	delete_file = 0 

	#indicates times the file has been restored
	restore_history = 'NULL'

	#create list of important data and open csv file
	databs = [[compr_path,era,era_type,obsnum,mdsum,jday,jdate,polarization,length,raw_path,cal_location,tape_location,compr_file_size,raw_file_size,compressed,ready_to_tape,delete_file,restore_history]]
	print databs 

	#write to csv file by item in list
	for item in databs:
		wr.writerow(item)
"""
#Load data into named database and table

# open a database connection
# be sure to change the host IP address, username, password and database name to match your own
connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)

# prepare a cursor object using cursor() method
cursor = connection.cursor()

print dbnum 
# execute the SQL query using execute() method.
cursor.execute('''LOAD DATA LOCAL INFILE '%s' INTO TABLE paperdata
COLUMNS TERMINATED BY ','
LINES TERMINATED BY '\n' '''%(dbo))

print 'Table data loaded.'

#Close and save changes to database
cursor.close()
connection.commit()
connection.close()

# exit the program
sys.exit()
'''
"""
