#!/usr/bin/python
# -*- coding: utf-8 -*-
# Update data in MySQL table 

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass
import shutil
import csv

### Script to delete files set by ISUS
### Looks through database for delete_file marker, deletes every directory and file in list

### Author: Immanuel Washington
### Date: 8-20-14

usrnm = raw_input('Root username: ')
pswd = getpass.getpass('Root password: ')

raw_value = 'ON TAPE'
deletion = []

failed_delete = '/data2/home/immwa/scripts/paperdata/failed_deletion.csv'
del_file = open(failed_delete,'wb')

#create 'writer' object
fd = csv.writer(del_file, dialect='excel')

# open a database connection
# be sure to change the host IP address, username, password and database name to match your own
connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)

# prepare a cursor object using cursor() method
cursor = connection.cursor()

#execute MySQL query
cursor.execute('SELECT julian_day, obsnum, raw_location, tape_location, delete_file from paperdata where delete_file = 1 order by julian_day asc')

#collects information from query
results = cursor.fetchall()

#results is a list of lists
for items in results:
	obsnum = items[1]
	if items[4] == 1 and not items[3] == 'NULL' and not items[2] == 'NULL':
		deletion.append([items[2],obsnum])

#loops through list and deletes raw files scheduled for deletion
confirm = raw_input('Are you sure you want to delete (y/n) ?: ')

#value to set delete_file to
del_value = 0
if confirm == 'y':
	for item in deletion:
		obsnum = item[1]
		try:
			shutil.rmtree(item[0])
		except:
			fd.writerow([item[0]])
			print 'ERROR: uv file %s not removed' %(item[0])
			continue

		if not os.path.isdir(item[0]):
			cursor.execute('''
			UPDATE paperdata
			SET delete_file = %d, raw_location = '%s'
			WHERE obsnum = %d;
			'''%(del_value, raw_value, obsnum)
		else:
			fd.writerow([item[0]])
			print 'ERROR: uv file %s not removed' %(item[0])
else:
	sys.exit()

print 'Table data updated.'

# Close and save changes to database
cursor.close()
connection.commit()
connection.close()

# exit the program
sys.exit()
