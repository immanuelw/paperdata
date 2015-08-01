#!/usr/bin/python
# -*- coding: utf-8 -*-
# Create PostgreSQL table 

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass
import pyganglia as pyg

### Script to recreate paperdata table format
### Opens MySQL through module, creates table through input name

### Author: Immanuel Washington
### Date: 8-20-14

def make_table(cursor, var_class):
	table_inst = ''
	cols = []
	for field in var_class.db_list:
		cols.append((field, var_class.db_inst[field][0], var_class.db_inst[field][1]))

	for col in cols:
		if col != col[-1]:
			table_inst += ' '.join((col[0], col[1], 'DEFAULT', col[2])) + ', '
		else:
			table_inst += ' '.join((col[0], col[1], 'DEFAULT', col[2]))

	cursor.execute('''CREATE TABLE IF NOT EXISTS %s (%s);''', (var_class.table, table_inst))

	print 'Table %s created' %(var_class.table)
	return None

if __name__ == '__main__':
	#inputs for user to access database
	usrnm = raw_input('Username: ')
	pswd = getpass.getpass('Password: ')
	table = raw_input('Create which table (monitor_files, iostat, cpu, ram, all)? :')

	# open a database connection
	# be sure to change the host IP address, username, password and database name to match your own
	connection = MySQLdb.connect(host = 'shredder', user = usrnm, passwd = pswd, db = 'ganglia', local_infile=True)

	# prepare a cursor object using cursor() method
	cursor = connection.cursor()

	if table == 'all':
		for var_class in udb.all_classes:
			var_class = udb.instant_class[table]
			make_table(cursor, var_class)
	else:
		var_class = udb.instant_class[table]
		make_table(cursor, var_class)

	# Close and Save database connection
	cursor.close()
	connection.commit()
	connection.close()
