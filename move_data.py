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

### Script to move and update paperdata database
### Moves .uvcRRE or .uv directory and updates path field in paperdata

### Author: Immanuel Washington
### Date: 8-20-14

def dupe_check(infile_list):
	#Load data into named database and table
	connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)
	cursor = connection.cursor()

	#Check if input file is in paperdata database
	cursor.execute('''SELECT path from paperdata''')
	resA = cursor.fetchall()
	cursor.execute('''SELECT raw_location from paperdata''')
	resB = cursor.fetchall()

	#convert tuples into list
	resC = []
	resR = []

	for item in resA:
		if item[0] != 'NULL':
			resC.append(item[0].split(':')[1])

	for item in resB:
		if item[0] != 'NULL':
			resR.append(item[0].split(':')[1])

	#empty list to add new files to
	new_data = []

	#Checks if file in database
	for item in infile_list:
		if item in resC or item in resR:
			continue
		else:
			print item
			nonDB_file = raw_input('File not in paperdata--Add file(a), Skip file(s), or Exit script(e)?: ')
			if nonDB_file == 's':
				continue
			elif nonDB_file == 'a':
				new_data.append(item)
			else:
				print 'Exiting...'
				sys.exit()

	cursor.close()
	connection.commit()
	connection.close()

	return new_data

def load_new_data(infile, new_data):
	#Directory of the infiles
	infile_dir = infile.split('z')[0]

	#If any new files exist
	if len(new_data) > 1:
		dbn = os.path.join(infile_dir, 'new_data.psv')
		dbf = os.path.join(infile_dir, 'false_data.psv')

		#writes to files with new information
		load_paperdata.gen_paperdata(new_data, dbn, dbf)

		#Loads new data into paperdata
		load_paperdata.load_db(dbn, usrnm, pswd)

	return None

def move_files(infile_list, outfile, move_data, usrnm, pswd):
	host = socket.gethostname()
	#Directory of the infiles
	infile_dir = infile_list[0].split('z')[0]

	#create file to log movement data	
	dbo = os.path.join(infile_dir, move_data)
	dbr = open(dbo,'wb')
	dbr.close()

	o_dict = {}
	for file in infile_list:
		zen = file.split('/')[-1]
		out = os.path.join(outfile,zen)
		o_dict.update({file:out})

	#Load data into named database and table
        connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)
        cursor = connection.cursor()

	#Load into db
	for infile in infile_list:
		if infile.split('.')[-1] != 'uv'and infile.split('.')[-1] != 'uvcRRE':
			print 'Invalid file type'
			sys.exit()

		outfile = o_dict[infile]

		#Opens file to append to
		dbr = open(dbo, 'a')
		wr = csv.writer(dbr, delimiter='|', dialect='excel')

		#moves file
		try:
			shutil.move(infile,outfile)
			wr.writerow([infile,outfile])
			print infile, outfile
			dbr.close()
		except:
			dbr.close()
			continue
		# execute the SQL query using execute() method, updates new location
		infile_path = host + ':' + infile
		outfile_path = host + ':' + outfile
		if infile.split('.')[-1] == 'uvcRRE':
			cursor.execute('''UPDATE paperdata set path = '%s' where path = '%s' '''%(outfile_path, infile_path))
		elif infile.split('.')[-1] == 'uv':
			cursor.execute('''UPDATE paperdata set raw_location = '%s', ready_to_tape = 1 where raw_location = '%s' '''%(outfile_path, infile_path))

	print 'File(s) moved and updated'

	#Close database and save changes
	cursor.close()
	connection.commit()
	connection.close()

	return None

if __name__ == '__main__':
	#output file
	time_date = time.strftime("%d-%m-%Y_%H:%M:%S")
	move_data = 'moved_data_%s.psv'%(time_date)

	usrnm = raw_input('Username: ')
	pswd = getpass.getpass('Password: ')

	#File information
	infile = raw_input('Full input path (EX: /data4/paper/feed/2456640/zen.*.uv*): ')
	outfile = raw_input('Output directory(EX: /data4/paper/raw_to_tape/2456640/): ')

	#checks to see that output dir is valid
	if not os.path.isdir(outfile):
	        print 'Output directory does not exist'
	        sys.exit()

	#checks for input file type
	file_type = raw_input('uv, uvcRRE, or both?: ')

	if not file_type in ['uv','uvcRRE','both']:
	        print 'Invalid file type'
	        sys.exit()

	#List of files in directory -- allowing mass movement of .uvcRRE and .uv files
	infile_list = glob.glob(infile)

	#Check for copies of files in database
	new_data = dupe_check(infile_list)

	#Loads new data into paperdata	
	load_new_data(infile, new_data)

	#Moves files and updates their location
	move_files(infile_list, outfile, move_data, usrnm, pswd)
