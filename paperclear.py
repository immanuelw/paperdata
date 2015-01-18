#!/usr/bin/python
# -*- coding: utf-8 -*-
# Delete data from MySQL table 

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass

usrnm = raw_input('Username: ')
pswd = getpass.getpass('Password: ')
# open a database connection
# be sure to change the host IP address, username, password and database name to match your own
connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)

# prepare a cursor object using cursor() method
cursor = connection.cursor()

# execute the SQL query using execute() method.
cursor.execute('''TRUNCATE table paperdata''')
print 'Table data deleted.' 

# Close and Save database
cursor.close()
connection.commit()
connection.close()
