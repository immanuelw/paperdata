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
usrnm = raw_input('Username: ')
pswd = getpass.getpass('Password: ')

# open a database connection
# be sure to change the host IP address, username, password and database name to match your own
connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)

# prepare a cursor object using cursor() method
cursor = connection.cursor()

# execute the SQL query using execute() method.
# Builds table by fields including defaults
cursor.execute('''CREATE TABLE paperjunk (
junk_path VARCHAR(100) DEFAULT NULL,
folio_path VARCHAR(100) DEFAULT NULL,
junk_size_bytes BIGINT DEFAULT 0,
usb_number INT DEFAULT 99,
renamed tinyint(1) DEFAULT 0);''')

print 'Table paperjunk created'

# Close and Save database connection
cursor.close()
connection.commit()
connection.close()

# exit the program
sys.exit()

