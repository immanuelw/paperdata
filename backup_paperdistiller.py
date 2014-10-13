#!/usr/bin/python
# -*- coding: utf-8 -*-
# Backup data from MySQL table 

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

usrnm = raw_input('Username: ')
pswd = getpass.getpass('Password: ')

time_date = time.strftime("%d-%m-%Y")

#Load data into named database and table

# open a database connection
# be sure to change the host IP address, username, password and database name to match your own
connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdistiller', local_infile=True)

# prepare a cursor object using cursor() method
cursor = connection.cursor()

# execute the SQL query using execute() method.
cursor.execute('SELECT julian_date from observation')
result = str(cursor.fetchone())
j_day = result[3:7]

#Instantiate file names
back1 = '/data2/home/immwa/scripts/paperdata/backups/paperdistiller_file_backup_%s_%s.csv'%(j_day,time_date)
back2 = '/data2/home/immwa/scripts/paperdata/backups/paperdistiller_observation_backup_%s_%s.csv'%(j_day,time_date)
back3 = '/data2/home/immwa/scripts/paperdata/backups/paperdistiller_neighbors_backup_%s_%s.csv'%(j_day,time_date)
back4 = '/data2/home/immwa/scripts/paperdata/backups/paperdistiller_log_backup_%s_%s.csv'%(j_day,time_date)

resultFile = open(back1,'wb+')
wr = csv.writer(resultFile, dialect='excel')

cursor.execute('SELECT * FROM file')
results = cursor.fetchall()

for item in results:
	#write to csv file by item in list
	wr.writerow(item)

resultFile.close()
resultFile = open(back2,'wb+')
wr = csv.writer(resultFile, dialect='excel')

cursor.execute('SELECT * FROM observation')
results = cursor.fetchall()

for item in results:
        #write to csv file by item in list
        wr.writerow(item)

resultFile.close()
resultFile = open(back3,'wb+')
wr = csv.writer(resultFile, dialect='excel')

cursor.execute('SELECT * FROM neighbors')
results = cursor.fetchall()

for item in results:
        #write to csv file by item in list
        wr.writerow(item)

resultFile.close()
resultFile = open(back4,'wb+')
wr = csv.writer(resultFile, dialect='excel')

cursor.execute('SELECT * FROM log')
results = cursor.fetchall()

for item in results:
        #write to csv file by item in list
        wr.writerow(item)

print time_date
print 'Database backup saved'

# Close connectin to database and save all changes
cursor.close()
connection.close()

# exit the program
sys.exit()
