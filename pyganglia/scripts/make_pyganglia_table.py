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
	cursor.execute('''CREATE TABLE IF NOT EXISTS monitor_files (
	filename VARCHAR(255) DEFAULT NULL,
	status VARCHAR(255) DEFAULT NULL,
	del_time INT DEFAULT 0,
	still_host VARCHAR(255) DEFAULT NULL,
	time_date DECIMAL(13,6) DEFAULT 0.000000);''')

	print 'Table monitor_files created'

	return None

def make_ram(cursor):
	cursor.execute('''CREATE TABLE IF NOT EXISTS ram (
	host VARCHAR(255) DEFAULT NULL,
	total BIGINT DEFAULT 0,
	used BIGINT DEFAULT 0,
	free BIGINT DEFAULT 0,
	shared BIGINT DEFAULT 0,
	buffers BIGINT DEFAULT 0,
	cached BIGINT DEFAULT 0,
	bc_used BIGINT DEFAULT 0,
	bc_free BIGINT DEFAULT 0,
	swap_total BIGINT DEFAULT 0,
	swap_used BIGINT DEFAULT 0,
	swap_free BIGINT DEFAULT 0,
	time_date DECIMAL(13,6) DEFAULT 0.000000);''')

	print 'Table ram created'

	return None

def make_iostat(cursor):
	cursor.execute('''CREATE TABLE IF NOT EXISTS iostat (
	host VARCHAR(255) DEFAULT NULL,
	device VARCHAR(100) DEFAULT NULL,
	tps DECIMAL(7,2) DEFAULT 0.00,
	read_s DECIMAL(7,2) DEFAULT 0.00,
	write_s DECIMAL(7,2) DEFAULT 0.00,
	bl_reads INT DEFAULT 0,
	bl_writes INT DEFAULT 0,
	time_date DECIMAL(13,6) DEFAULT 0.000000);''')

	print 'Table iostat created'

	return None

def make_cpu(cursor):
	cursor.execute('''CREATE TABLE IF NOT EXISTS cpu (
	host VARCHAR(255) DEFAULT NULL,
	cpu INT DEFAULT 0,
	user_perc DECIMAL(5,2) DEFAULT 0.00,
	sys_perc DECIMAL(5,2) DEFAULT 0.00,
	iowait_perc DECIMAL(5,2) DEFAULT 0.00,
	idle_perc DECIMAL(5,2) DEFAULT 0.00,
	intr_s INT DEFAULT 0,
	time_date DECIMAL(13,6) DEFAULT 0.000000);''')

	print 'Table cpu created'

	return None

if __name__ == '__main__':
	#inputs for user to access database
	usrnm = raw_input('Username: ')
	pswd = getpass.getpass('Password: ')
	if len(sys.argv) > 1:
		table = sys.argv[1]
	else:
		table = raw_input('Create which table (monitor_files, ram, iostat, cpu, all)? :')

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
	elif table == 'all':
		make_monitor_files(cursor)
		make_ram(cursor)
		make_iostat(cursor)
		make_cpu(cursor)

	# Close and Save database connection
	cursor.close()
	connection.commit()
	connection.close()
