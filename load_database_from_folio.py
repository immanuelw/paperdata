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
import aipy as A

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
	for x in ['bytes','KB','MB','GB']:
		if num < 1024.0:
			return "%3.1f%s" % (num, x)
		num /= 1024.0
	return "%3.1f%s" % (num, 'TB')

#User input information
db = raw_input('32, 64, or 128?: ')

datab = 'paperdata'
usrnm = raw_input('Username: ')
pswd = getpass.getpass('Password: ')

data32 = '/data4/raw_data/'
data64 = '/data4/paper/2012EoR/psa_live/'
data128 = '/data4/paper/still_raw_data_test/'

db32 = '/data2/home/immwa/scripts/paper_output/db_output32.csv'
db64 = '/data2/home/immwa/scripts/paper_output/db_output64.csv'
db128 = '/data2/home/immwa/scripts/paper_output/db_output128.csv'

host = 'folio'

#searches for only particular files
if db == '32':
	datanum = data32
	dbnum = db32
elif db == '64':
	datanum = data64
	dbnum = db64
elif db == '128':
	datanum = data128
	dbnum = db128

#combined all eras into one table
table_name = 'paperdata'

resultFile = open(dbnum,'wb')

#create 'writer' object
wr = csv.writer(resultFile, dialect='excel')

#create csv file to log bad files
error_file = open('/data2/home/immwa/scripts/paper_output/', 'a')
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


#iterates through directories, listing information about each one
for root, dirs, files in os.walk(datanum):
	#brute force check to avoid other files within searched directories
	if db =='32':
		datatruth = len(root) > 26 and len(root) < 34 and root[16] =='p'
	elif db == '64':
		datatruth = len(root) > 36 and len(root) < 64 and root[30] == 'p'
	elif db == '128':
		#need to change to 128 specifications
		#datatruth = len(root) > 36 and len(root) < 64 and root[30] == 'p'
		datatruth = len(root) >	15

	if datatruth:
		for dir in dirs:
			#if filename ends with uvcRRE, record into file
			if dir[-6:] == 'uvcRRE' and len(dir) > 6:
				#indicates name of full directory
				path = os.path.join(root, dir)
				print path

				#indicates size of file
				sz = sizeof_fmt(get_size(path))

				#checks a .uv file for data
				visdata = os.path.join(path, 'visdata')
				if not os.path.isfile(visdata):
					error_list = [[path,'No visdata']]
					for item in error_list:
						ewr.writerow(item)
					continue

                                #allows uv access
				try:
	                               uv = A.miriad.UV(path)
				except:
					error_list = [[path,'Cannot access .uv file']]
                                        for item in error_list:
                                                ewr.writerow(item)
					continue	

				#indicates julian date
				jdate = uv['time']


				#indicates julian day
				if datanum == data32:
					jday = int(str(jdate)[4:7])
				elif datanum == data64:
					jday = int(str(jdate)[3:7])
				elif datanum == data128:
					jday = int(str(jdate)[3:7])	

				#indicates set of data used
				if datanum == data32:
					era = 32
				elif datanum == data64:
					era = 64
				elif datanum == data128:
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

				#indicates length of information in file
				#length = uv['inttime'] 

				t_min = 0
				t_max = 0
				n_times = 0
				c_time = 0

				for (uvw, t, (i,j)),d in uv.all():
					if t_min == 0 or t < t_min:
						t_min = t
					if t_max == 0 or t > t_max:
                                                t_max = t
					if c_time != t:
						c_time = t
						n_times += 1

				if n_times > 1:
					dt = -(t_min - t_max)/(n_times - 1)
				else:
					dt = -(t_min - t_max)/(n_times)

				length = n_times * dt
				#round so fits obsnum
				length = round(length, 5)

				#variable to input into jdpol2obsnum
				divided_jdate = length

				#gives each file unique id
				if length > 0:
					obsnum = jdpol2obsnum(jdate,polarization,divided_jdate)
				else:
					obsnum = 0

				#location of raw files
				raw_location = path[:-4] #assume in same directory

				#location of calibrate files
                                if datanum == data32:
					cal_location = '/usr/global/paper/capo/arp/calfiles/psa898_v003.py'
                                elif datanum == data64:
					cal_location = '/usr/global/paper/capo/zsa/calfiles/psa6240_v003.py'
				elif datanum == data128:
					cal_location = 'NULL'

				#indicates if file is compressed
				compressed = 1

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

#Load data into named database and table

# open a database connection
# be sure to change the host IP address, username, password and database name to match your own
connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = datab, local_infile=True)

# prepare a cursor object using cursor() method
cursor = connection.cursor()

print dbnum 
# execute the SQL query using execute() method.
cursor.execute('''USE paperdata;
LOAD DATA LOCAL INFILE '%s' INTO TABLE %s
COLUMNS TERMINATED BY ','
LINES TERMINATED BY '\n' '''%(dbnum, table_name))

print 'Table data loaded.'

# close the cursor object
cursor.close()

#save changes to database
connection.commit()

# close the connection
connection.close()

# exit the program
sys.exit()
