#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass
import time
import csv
import base64

### Script to Backup paperdata database
### Finds time and date and writes table into .csv file

### Author: Immanuel Washington
### Date: 8-20-14

datab = 'paperdata'
#usrnm = raw_input('Username: ')
#pswd = getpass.getpass('Password: ')

usrnm = 'immwa'
pswd = base64.b64decode('aW1td2EzOTc4')

time_date = time.strftime("%d-%m-%Y_%H:%M:%S")

dbnum = '/data2/home/immwa/scripts/paperdata/backups/paperjunk_backup_%s.csv'%(time_date)

print dbnum
resultFile = open(dbnum,'wb+')

#create 'writer' object
wr = csv.writer(resultFile, dialect='excel')

#Load data into named database and table

# open a database connection
# be sure to change the host IP address, username, password and database name to match your own
connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = datab, local_infile=True)

# prepare a cursor object using cursor() method
cursor = connection.cursor()

# execute the SQL query using execute() method.
cursor.execute('SELECT * FROM paperjunk order by julian_date asc, raw_location asc, path asc')
results = cursor.fetchall()

for item in results:
	#write to csv file by item in list
	wr.writerow(item)

print time_date
print 'Table data backup saved'

# Close the cursor object
cursor.close()
connection.close()

# exit the program
sys.exit()
