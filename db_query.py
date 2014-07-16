#!/usr/bin/python
# -*- coding: utf-8 -*-
# Delete data from MySQL table 

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass

datab = raw_input('Database:')
pswd = getpass.getpass('Password:')
qry = sys.argv[1]
# open a database connection
# be sure to change the host IP address, username, password and database name to match your own
connection = MySQLdb.connect (host = 'shredder', passwd = pswd, db = datab, local_infile=True)

# prepare a cursor object using cursor() method
cursor = connection.cursor()

print sys.argv[1]
# execute the SQL query using execute() method.
cursor.execute('%s'%(qry))

#finds all rows outputted by query, prints them
results = cursor.fetchall()
print results
#complete
print 'Query Complete'

# close the cursor object
cursor.close()

#save changes to database
connection.commit()

# close the connection
connection.close()

# exit the program
sys.exit()

