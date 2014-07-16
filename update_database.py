#!/usr/bin/python
# -*- coding: utf-8 -*-
# Loa data into MySQL table 

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass

datab = raw_input('Database:')
table = raw_input('Update data in table named:')
pswd = getpass.getpass('Password:')

mini = raw_input('Minimum Julian Date:')
maxi = raw_input('Maximum Julian Date:')

compressed = 'compressed'
obsnum_string = 'obsnum'

#need way to get compr_value and obsnum from paperdistiller 

# open a database connection
connection = MySQLdb.connect (host = 'shredder', passwd = pswd, db = 'paperdistiller', local_infile=True)

cursor = connection.cursor()

# execute the SQL query using execute() method.
cursor.execute('SELECT obsnum, status from observations where julian_date >= %d and julian date <= %d;'%(mini, maxi))

#collects information from query
results = cursor.fetchall()

#close, save and end connection
cursor.close()
connection.commit()
connection.close()

# open a database connection
# be sure to change the host IP address, username, password and database name to match your own
connection = MySQLdb.connect (host = 'shredder', passwd = pswd, db = datab, local_infile=True)

# prepare a cursor object using cursor() method
cursor = connection.cursor()

#results is a list of lists
for items in results:
        obsnum = items[0]
        if items[1] == 'COMPLETE':
                compr_value = True
        else:
                compr_value = False

	# execute the SQL query using execute() method.
	cursor.execute('''
	UPDATE %s
	SET %s = %s
	WHERE %s = %d;
	'''%(table, compressed, compr_value, obsnum_string, obsnum)) 
	###change so %d if number or %s if string entry!!!
print 'Table data updated.'

# close the cursor object
cursor.close()

#save changes to database
connection.commit()

# close the connection
connection.close()

# exit the program
sys.exit()
