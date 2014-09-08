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

#resultFile = open(dbnum,'wb')

#create 'writer' object
#wr = csv.writer(resultFile, dialect='excel')

#create csv file to log bad files
error_file = open('/data2/home/immwa/scripts/paper_output/error_%s.log'%(db), 'a')
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
		datatruth = len(root) > 26 and root[16] =='p'
	elif db == '64':
		datatruth = len(root) > 36 and root[30] == 'p'
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
					print error_list
					continue

                                #allows uv access
				try:
	                               uv = A.miriad.UV(path)
				except:
					error_list = [[path,'Cannot access .uv file']]
                                        for item in error_list:
                                                ewr.writerow(item)
						print error_list
					continue	

#				#write to csv file by item in list
#				for item in databs:
#					wr.writerow(item)

sys.exit()
