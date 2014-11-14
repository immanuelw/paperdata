#!/usr/bin/python
# -*- coding: utf-8 -*-
# Find Errors of .uvcRRE files

# import the MySQLdb and sys modules
import MySQLdb
import getpass
import os
import csv
import aipy as A
import glob
import time

### Script to find errors of .uvcRRE files from folio
### Crawls folio and reads through .uvcRRE files to find erros

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

#User input information
datanum = raw_input('Full path of .uvcRRE files: ')

#Former location of .uvcRRE files

#create csv file to log bad files
tune = time.strftime("%d-%m-%Y")
error_file = open('/data2/home/immwa/scripts/paper_output/error_%s.log'%(tune), 'a')
ewr = csv.writer(error_file, dialect='excel')

#iterates through directories, listing information about each one
dirs = glob.glob(datanum)
	for dir in dirs:
		#indicates name of full directory
		path = dir
		print path

		#indicates size of file in bytes
		sz = get_size(path)
		#arbitrary low number -- means file does not have any information?
		if sz <= 10000:
			error_list = [[path, 'Not enough data']]
			for item in error_list:
				ewr.writerow(item)
				print error_list

		#temp fix
	        if dir == '/nas2/data/psa6668/zen.2456668.17386.yx.uvcRRE':
	                error_list = [[path,'Unknown error']]
	                for item in error_list:
	                        ewr.writerow(item)
	                continue

	        #checks a .uv file for data
	        visdata = os.path.join(path, 'visdata')
	        if not os.path.isfile(visdata):
	                error_list = [[path,'No visdata']]
	                for item in error_list:
	                        ewr.writerow(item)
	                continue

	        #checks a .uv file for vartable
	        vartable = os.path.join(path, 'vartable')
	        if not os.path.isfile(vartable):
	                error_list = [[path,'No vartable']]
	                for item in error_list:
	                        ewr.writerow(item)
	                continue

	        #checks a .uv file for header
	        header = os.path.join(path, 'header')
	        if not os.path.isfile(header):
	                error_list = [[path,'No header']]
	                for item in error_list:
	                        ewr.writerow(item)

		#allows uv access
		try:
			uv = A.miriad.UV(path)
		except:
			error_list = [[path,'Cannot access .uv file']]
			for item in error_list:
				ewr.writerow(item)
			print error_list
			continue	
