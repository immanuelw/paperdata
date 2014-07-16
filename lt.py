#!/usr/bin/python
# -*- coding: utf-8 -*-
# Delete data from MySQL table 

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass

pswd = getpass.getpass('Password:')
qry = sys.argv[1]
# open a database connection
# be sure to change the host IP address, username, password and database name to match your own
connection = MySQLdb.connect (host = 'localhost', user = 'root', passwd = pswd, db = 'test_folio', local_infile=True)

# prepare a cursor object using cursor() method
cursor = connection.cursor()

print sys.argv[1]
# execute the SQL query using execute() method.
cursor.execute('Use test_folio')
cursor.execute('%s'%(qry))

#finds all rows outputted by query, prints them
results = cursor.fetchall()

widths = []
columns = []
tavnit = '|'
separator = '+' 

for cd in cursor.description:
	widths.append(max(cd[2], len(cd[0])))
	columns.append(cd[0])

for w in widths:
	tavnit += " %-"+"%ss |" % (w,)
	separator += '-'*w + '--+'

print(separator)
print(tavnit % tuple(columns))
print(separator)
for row in results:
	print(tavnit % row)
print(separator)

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
