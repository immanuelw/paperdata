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

### Script to move and update paperdata database
### Moves .uvcRRE directory and updates path field in paperdata

### Author: Immanuel Washington
### Date: 8-20-14

usrnm = raw_input('Username: ')
pswd = getpass.getpass('Password: ')

#File information
host = socket.gethostname()
infile = raw_input('Full input path: ')
outfile = raw_input('Output directory: ')

#dbo = '/data2/home/immwa/scripts/paper_output/move_db.csv'
dbo = os.path.join(outfile,'moved_data.csv')
resultFile = open(dbo,'wb')

#create 'writer' object
wr = csv.writer(resultFile, dialect='excel')

#List of files in directory -- allowing mass movement of .uvcRRE files
infile_list = glob.glob(infile)

o_dict = {}
for file in infile_list:
	zen = file.split('/')[-1]
	out = os.path.join(outfile,zen)
	o_dict.update({file:out})

#Load data into named database and table

# open a database connection
# be sure to change the host IP address, username, password and database name to match your own
connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)

# prepare a cursor object using cursor() method
cursor = connection.cursor()

for infile in infile_list:
	outfile = o_dict[infile]
	#moves file
	try:
		shutil.move(infile,outfile)
		wr.writerow([infile,outfile])
		print infile, outfile

	except:
		continue
	# execute the SQL query using execute() method, updates new location
	infile_path = host + ':' + infile
	outfile_path = host + ':' + outfile
	if infile.split('.')[-1] == 'uvcRRE':
		cursor.execute('''UPDATE paperdata set path = '%s' where path = '%s' '''%(outfile_path, infile_path))
	elif infile.split('.')[-1] == 'uv':
		cursor.execute('''UPDATE paperdata set raw_location = '%s' where raw_location = '%s' '''%(outfile_path, infile_path))

print 'File(s) moved and updated'
#Close database and save changes
cursor.close()
connection.commit()
connection.close()

# exit the program
sys.exit()
