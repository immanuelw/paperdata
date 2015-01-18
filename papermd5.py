#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass
import os
import csv
import hashlib
import glob
import base64

### Script to update paperdistiller with md5sums
### Queries paperdistiller for files without md5sums

### Author: Immanuel Washington
### Date: 12-09-14

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

def create_md5(dbo, dbe, usrnm, pswd):

	#Removes all files from list that already exist in the database
	connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdistiller', local_infile=True)

	cursor = connection.cursor()
	cursor.execute('''SELECT filename, md5sum from file where md5sum is NULL''')
	results = cursor.fetchall()

	#Update database with new md5sums
	for item in results:
		data_file = open(dbo,'ab')
		wr = csv.writer(data_file, delimiter='|', lineterminator='\n', dialect='excel')
		#create csv file to log bad files
		error_file = open(dbe, 'ab')
		ewr = csv.writer(error_file, delimiter='|', lineterminator='\n', dialect='excel')

		filename = item[0]
		if os.path.isdir(filename):
			try:
				md5 = md5sum(filename)
				wr.writerow([filename,md5])
				data_file.close()
			except:
				ewr.writerow([filename, 'Failed generating md5sum'])
				error_file.close()
				continue
			cursor.execute('''UPDATE file SET md5sum = '%s' WHERE filename = '%s' ''' %(md5, filename))
		else:
			ewr.writerow([filename, 'Path no longer exists'])
			error_file.close()
			continue
	
	cursor.close()
	connection.commit()
	connection.close()

	return None

def papermd5(auto):
	#User input information
	if auto != 'y':
		usrnm = raw_input('Username: ')
		pswd = getpass.getpass('Password: ')

	else:
		usrnm = 'jaguirre'
		pswd = base64.b64decode('amFndWlycmU2OTE5')

	dbo = '/data4/paper/paperoutput/md5.psv'
	dbe = '/data4/paper/paperoutput/err_md5.psv'

	#Creates md5sum and updates database
	create_md5(dbo, dbe, usrnm, pswd)

if __name__ == '__main__':
	auto = 'n'
	papermd5(auto)
