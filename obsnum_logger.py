#!/usr/bin/python
# -*- coding: utf-8 -*-
# Update data in MySQL table 

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass
import csv

#table = 'paperdata'
table = raw_input('Input table name: ')
usrnm = raw_input('Root username: ')
pswd = getpass.getpass('Root password: ')

raw = 'raw_location'
obsnum_string = 'obsnum'
delt = 'ready_to_delete'

#filename = '/data2/home/immwa/scripts/paper/jd_obsnum.csv'
filename = '/data2/home/immwa/scripts/paper/jd_obsnum_%s.csv'%(table)

resultFile = open(filename,'wb')
wr = csv.writer(resultFile, dialect='excel')

# open a database connection
# be sure to change the host IP address, username, password and database name to match your own
connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)

# prepare a cursor object using cursor() method
cursor = connection.cursor()

#execute MySQL query
#cursor.execute('SELECT julian_date, obsnum from paperdata order by julian_day')

cursor.execute('SELECT julian_date, obsnum from %s order by julian_date' %(table))

#collects information from query
results = cursor.fetchall()

#results is a list of lists
for items in results:
	wr.writerow(items)
print 'Obsnums logged'

# close the cursor object
cursor.close()

#save changes to database
connection.commit()

# close the connection
connection.close()

# exit the program
sys.exit()
