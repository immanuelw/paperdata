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
qry = sys.argv[1]
# open a database connection
# be sure to change the host IP address, username, password and database name to match your own
connection = MySQLdb.connect (host = 'shredder', passwd = pswd, db = datab, local_infile=True)

# prepare a cursor object using cursor() method
cursor = connection.cursor()

print sys.argv[1]
# execute the SQL query using execute() method.
cursor.execute('''USE paperdata;
UPDATE %s
SET %s = %s
WHERE %s = %s;
'''%(table, compressed, compr_value, obs_string, obsnum)) 
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
