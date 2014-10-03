#!/usr/bin/python
# -*- coding: utf-8 -*-
# Create MySQL table 

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass

### Script to recreate paperdata table format
### Opens MySQL through module, creates table through input name

### Author: Immanuel Washington
### Date: 8-20-14

#inputs for user to access database
table = raw_input('Create table named: ')
usrnm = raw_input('Username: ')
pswd = getpass.getpass('Password: ')

# open a database connection
# be sure to change the host IP address, username, password and database name to match your own
connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)

# prepare a cursor object using cursor() method
cursor = connection.cursor()

# execute the SQL query using execute() method.
# Builds table by fields including defaults
cursor.execute('''CREATE TABLE %s (
path VARCHAR(100) DEFAULT NULL,
era INT DEFAULT NULL,
era_type VARCHAR(100) DEFAULT NULL,
obsnum BIGINT(20) DEFAULT NULL,
julian_day INT DEFAULT NULL,
julian_date DECIMAL(16,8) DEFAULT NULL,
polarization VARCHAR(4) DEFAULT NULL,
data_length DECIMAL(20,15) DEFAULT NULL,
raw_location VARCHAR(100) DEFAULT NULL,
cal_location VARCHAR(100) DEFAULT NULL,
tape_location VARCHAR(100) DEFAULT NULL,
compr_file_size DECIMAL(6,2) DEFAULT NULL,
raw_file_size DECIMAL(10,2) DEFAULT NULL,
compressed BOOLEAN DEFAULT FALSE,
ready_to_tape BOOLEAN DEFAULT FALSE,
delete_file BOOLEAN DEFAULT FALSE);''' %(table))

# fetch a single row using fetchone() method.
row = cursor.fetchone()

print 'Table %s created' %(table)

# close the cursor object
cursor.close()

#save changes to database
connection.commit()

# close the connection
connection.close()

# exit the program
sys.exit()

