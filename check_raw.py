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

#User input information
db = raw_input('32, 64, or 128?: ')

usrnm = raw_input('Username: ')
pswd = getpass.getpass('Password: ')
datab = 'paperdata'

data32 = '/data4/raw_data/'
data64 = '/mnt/MartinVanBuren/'
#data64 = '/mnt/MartinVanBuren/data/'
data128 = '/data4/paper/still_raw_data_test/'

db32 = '/data2/home/immwa/scripts/paper_output/db_o32.csv'
db64 = '/data2/home/immwa/scripts/paper_output/db_o64.csv'
db128 = '/data2/home/immwa/scripts/paper_output/db_o128.csv'

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
error_file = open('/data2/home/immwa/scripts/paper_output/dboE%s.csv'%(db), 'wb')
ewr = csv.writer(error_file, dialect='excel')

#counts amount of uv files with data
count = 0

#iterates through directories, listing information about each one
for root, dirs, files in os.walk(datanum):
	#brute force check to avoid other files within searched directories
	if db =='32':
		datatruth = len(root) > 26 and len(root) < 34 and root[16] =='p'
	elif db == '64':
		datatruth = len(root) > 14
	elif db == '128':
		#need to change to 128 specifications
		#datatruth = len(root) > 36 and len(root) < 64 and root[30] == 'p'
		datatruth = len(root) >	15

	if datatruth:
		for dir in dirs:
			#indicates name of full directory
			path = os.path.join(root, dir)
			print path

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
			#jdate = uv['time']
			#print jdate

			count += 1

			file = os.path.join('/data4/paper/2012EoR/psa_live/',path[-28:])
			file += 'cRRE'
			#print file

			#Load data into named database and table

			# open a database connection
			# be sure to change the host IP address, username, password and database name to match your own
			connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = datab, local_infile=True)

			# prepare a cursor object using cursor() method
			cursor = connection.cursor()

			# execute the SQL query using execute() method.
			cursor.execute('''SELECT path from paperdata where path = '%s' '''%(file))

			results = cursor.fetchall()
			print results

			raw_loc = 'LOCATION NOT FOUND'
			for item in results:
				if os.path.isdir(item[0]):
					raw_loc = path
				else:
	                                error_list = [[path,'No .uvcRRE file exists in database']]
        	                        for item in error_list:
                	                        ewr.writerow(item)
	                                continue

			#print raw_loc

			#print '''UPDATE paperdata SET raw_location = '%s' WHERE path = '%s' '''%(raw_loc,file)

			cursor.execute('''
			UPDATE paperdata
			SET raw_location = '%s'
			WHERE path = '%s' '''%(raw_loc,file))

			# close the cursor object
			cursor.close()

			#save changes to database
			connection.commit()

			# close the connection
			connection.close()

print count
# exit the program
sys.exit()
