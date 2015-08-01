#!/usr/bin/python
# -*- coding: utf-8 -*-
# Delete data from MySQL table 

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass

def clear_ganglia(usrnm, pswd):
	# open a database connection
	# be sure to change the host IP address, username, password and database name to match your own
	connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'ganglia', local_infile=True)

	# prepare a cursor object using cursor() method
	cursor = connection.cursor()

	# execute the SQL query using execute() method.
	cursor.execute('''TRUNCATE table monitor_files; TRUNCATE table iostat; TRUNCATE table ram; TRUNCATE table cpu;''')
	print 'Table data deleted.' 

	# Close and Save database
	cursor.close()
	connection.commit()
	connection.close()

	return None

if __name__ == '__main__':
	usrnm = raw_input('Username: ')
	pswd = getpass.getpass('Password: ')
	clear_ganglia(usrnm, pswd)
