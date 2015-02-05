#!/usr/bin/python
# -*- coding: utf-8 -*-
# Create MySQL table 

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass

### Script to recreate pyganglia table format
### Opens MySQL through module, creates table through input name

### Author: Immanuel Washington
### Date: 8-20-14

def make_monitor_files(cursor):
	# execute the SQL query using execute() method.
	# Builds table by fields including defaults
	cursor.execute('''CREATE TABLE monitor_files (
	filename VARCHAR(255) DEFAULT NULL,
	status VARCHAR(255) DEFAULT NULL,
	del_time INT DEFAULT 0,
	still_host VARCHAR(255) DEFAULT NULL,
	time_date VARCHAR(255) DEFAULT NULL);''')

	print 'Table monitor_files created'

	return None

def make_ram(cursor):
	cursor.execute('''CREATE TABLE ram (
	host VARCHAR(255) DEFAULT NULL,
	total INT DEFAULT 0,
	used INT DEFAULT 0,
	free INT DEFAULT 0,
	shared INT DEFAULT 0,
	buffers INT DEFAULT 0,
	cached INT DEFAULT 0,
	bc_used INT DEFAULT 0,
	bc_free INT DEFAULT 0,
	swap_total INT DEFAULT 0,
	swap_used INT DEFAULT 0,
	swap_free INT DEFAULT 0,
	time_date VARCHAR(255) DEFAULT NULL);''')

	print 'Table ram created'

	return None

def make_iostat(cursor):
	cursor.execute('''CREATE TABLE iostat (
	host VARCHAR(255) DEFAULT NULL,
	device VARCHAR(100) DEFAULT NULL,
	tps DECIMAL(7,2) DEFAULT 0.00,
	read_s DECIMAL(7,2) DEFAULT 0.00,
	write_s DECIMAL(7,2) DEFAULT 0.00,
	reads INT DEFAULT 0,
	writes INT DEFAULT 0,
	time_date VARCHAR(255) DEFAULT NULL);''')

	print 'Table iostat created'

	return None

def make_cpu(cursor):
	cursor.execute('''CREATE TABLE cpu (
	host VARCHAR(255) DEFAULT NULL,
	cpu INT DEFAULT 0,
	user_perc DECIMAL(5,2) DEFAULT 0.00,
	sys_perc DECIMAL(5,2) DEFAULT 0.00,
	iowait_perc DECIMAL(5,2) DEFAULT 0.00,
	idle_perc DECIMAL(5,2) DEFAULT 0.00,
	intr_s INT DEFAULT 0,
	time_date VARCHAR(255) DEFAULT NULL);''')

	print 'Table cpu created'

	return None

if __name__ == '__main__':
	#inputs for user to access database
	usrnm = raw_input('Username: ')
	pswd = getpass.getpass('Password: ')
	table = raw_input('Create which table (monitor_files, ram, iostat, cpu)? :')

	# open a database connection
	# be sure to change the host IP address, username, password and database name to match your own
	connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'ganglia', local_infile=True)

	# prepare a cursor object using cursor() method
	cursor = connection.cursor()

	if table == 'monitor_files':
		make_monitor_files(cursor)
	elif table == 'ram':
		make_ram(cursor)
	elif table == 'iostat':
		make_iostat(cursor)
	elif table == 'cpu':
		make_cpu(cursor)

	# Close and Save database connection
	cursor.close()
	connection.commit()
	connection.close()
