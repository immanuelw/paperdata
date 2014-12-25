#!/usr/bin/python
# -*- coding: utf-8 -*-
# Move data on folio and update paperdata database with new location

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass
import shutil
import glob
import socket
import csv
import os
import time
import load_paperdata
import move_data

### Script to move and update paperdata database
### Moves .uvcRRE or .uv directory and updates path field in paperdata

### Author: Immanuel Washington
### Date: 8-20-14

if __name__ == '__main__':
	#output file
	time_date = time.strftime("%d-%m-%Y_%H:%M:%S")
	move_data = 'moved_data_%s.psv'%(time_date)

	usrnm = raw_input('Username: ')
	pswd = getpass.getpass('Password: ')

	#checks for input file type
	file_type = raw_input('uv or uvcRRE?: ')
	#file_type = 'uvcRRE'

	if not file_type in ['uv','uvcRRE']:
		print 'Invalid file type'
		sys.exit()

	#File information
	connection = MySQLdb.connect (host = 'shredder', user = 'paperboy', passwd = 'paperboy', db = 'paperdata', local_infile=True)
	cursor = connection.cursor()

	if file_type == 'uvcRRE':
		cursor.execute('''SELECT SUBSTRING_INDEX(path, 'z', 1), julian_day from paperdata where path like '%/raw_data/psa%' group by SUBSTRING_INDEX(path, 'z', 1) order by julian_day asc''')
	elif file_type == 'uv':
		cursor.execute('''SELECT SUBSTRING_INDEX(raw_path, 'z', 1), julian_day from paperdata where raw_path like '%/paper/nas1_data/psa%' group by SUBSTRING_INDEX(raw_path, 'z', 1) order by julian_day asc''')
	jdays = cursor.fetchall()
	cursor.close()
	connection.close()

	for jday in jdays:
		if file_type == 'uvcRRE':
			infile = os.path.join(jday[0].split(':')[1], 'zen.*.uvcRRE')
			outfile = os.path.join('/data4/paper/2011EoR/', '245' + str(jday[1]))
			print infile, outfile
		elif file_type == 'uv':
			infile = os.path.join(jday[0].split(':')[1], 'zen.*.uv')
			outfile = os.path.join('/data4/paper/raw_to_tape/', '245' + str(jday[1]))
			print infile, outfile

		#checks to see that output dir is valid
		if not os.path.isdir(outfile):
			print 'Output directory does not exist'
			os.mkdir(outfile)

		#List of files in directory -- allowing mass movement of .uvcRRE and .uv files
		infile_list = glob.glob(infile)
		infile_list.sort()

		#Check for copies of files in database
		new_data = move_data.dupe_check(infile_list)

		#Loads new data into paperdata	
		move_data.load_new_data(infile, new_data)

		#Moves files and updates their location
		if file_type in ['uv']:
			move_data.move_files(infile_list, outfile, move_data, usrnm, pswd)
		elif file_type in ['uvcRRE']:
			move_data.move_compressed_files(infile_list, outfile, move_data, usrnm, pswd)
