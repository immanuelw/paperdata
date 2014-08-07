#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass
import time
import csv
import shutil

datab = 'paperdata'
usrnm = raw_input('Username: ')
pswd = getpass.getpass('Password: ')

table = 'paperdata' 

infile = raw_input('Full input path: ')
outfile = raw_input('Full output path: ')

#Load data into named database and table

# open a database connection
# be sure to change the host IP address, username, password and database name to match your own
connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = datab, local_infile=True)

# prepare a cursor object using cursor() method
cursor = connection.cursor()

# execute the SQL query using execute() method, updates new location
cursor.execute('UPDATE paperdata set path = %s where path = %s'%(outfile, infile))

#moves file
shutil.move(infile,outfile)

print 'File moved and updated'
# close the cursor object
cursor.close()

#save changes to database
connection.commit()

# close the connection
connection.close()

# exit the program
sys.exit()
