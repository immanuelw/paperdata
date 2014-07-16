#!/usr/bin/python
# -*- coding: utf-8 -*-
# Create MySQL table 

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass

table = raw_input('Create table named:')
datab = raw_input('Database:')
pswd = getpass.getpass('Password:')
qry = sys.argv[1]
# open a database connection
# be sure to change the host IP address, username, password and database name to match your own
connection = MySQLdb.connect (host = 'shredder', passwd = pswd, db = datab, local_infile=True)

# prepare a cursor object using cursor() method
cursor = connection.cursor()

# execute the SQL query using execute() method.
cursor.execute('''CREATE TABLE %s (
filename VARCHAR(100) DEFAULT NULL,
location VARCHAR(100) DEFAULT NULL,
antennas INT DEFAULT NULL,
obsnum INT DEFAULT NULL,
julian_day INT DEFAULT NULL,
julian_date DECIMAL(12,5) DEFAULT NULL,
polarization CHAR(2) DEFAULT NULL,
data_length VARCHAR(10) DEFAULT NULL,
raw_location VARCHAR(100) DEFAULT NULL,
cal_location VARCHAR(100) DEFAULT NULL,
file_size VARCHAR(15) DEFAULT NULL);''' %(table))

# fetch a single row using fetchone() method.
row = cursor.fetchone()

# print the row[0]
# (Python starts the first row in an array with the number zero – instead of one)
print 'Table %s created' %(table)

# close the cursor object
cursor.close()

#save changes to database
connection.commit()

# close the connection
connection.close()

# exit the program
sys.exit()

