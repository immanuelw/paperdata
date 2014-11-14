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

### Script to move and update paperdata database
### Moves .uvcRRE directory and updates path field in paperdata

### Author: Immanuel Washington
### Date: 8-20-14

usrnm = raw_input('Username: ')
pswd = getpass.getpass('Password: ')

#File information
host = socket.gethostname()
infile = raw_input('Full input path: ')
outfile = raw_input('Full output path: ')

#List of files in directory -- allowing mass movement of .uvcRRE files
infile_list = glob.glob(infile)

#Load data into named database and table

# open a database connection
# be sure to change the host IP address, username, password and database name to match your own
connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)

# prepare a cursor object using cursor() method
cursor = connection.cursor()

for infile in infile_list:
	#moves file
	try:
		shutil.move(infile,outfile)
	except:
		continue
	# execute the SQL query using execute() method, updates new location
	infile_path = host + ':' + infile
	outfile_path = host + ':' + outfile
	cursor.execute('UPDATE paperdata set path = %s where path = %s'%(outfile_path, infile_path))

print 'File(s) moved and updated'
#Close database and save changes
cursor.close()
connection.commit()
connection.close()

# exit the program
sys.exit()
