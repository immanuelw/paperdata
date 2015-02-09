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

def make_paperdata(cursor):
	# execute the SQL query using execute() method.
	# Builds table by fields including defaults
	cursor.execute('''CREATE TABLE paperdata (
	path VARCHAR(100) DEFAULT NULL,
	era INT DEFAULT 0,
	era_type VARCHAR(100) DEFAULT NULL,
	obsnum BIGINT(20) DEFAULT 0,
	md5sum VARCHAR(32) DEFAULT NULL,
	julian_day INT DEFAULT 0,
	julian_date DECIMAL(12,5) DEFAULT 0.00000,
	polarization VARCHAR(4) DEFAULT NULL,
	data_length DECIMAL(20,19) DEFAULT 0.0000000000000000000,
	raw_path VARCHAR(100) DEFAULT NULL,
	cal_path VARCHAR(100) DEFAULT NULL,
	npz_path VARCHAR(100) DEFAULT NULL,
	final_product_path VARCHAR(100) DEFAULT NULL,
	tape_index VARCHAR(100) DEFAULT NULL,
	compr_file_size_MB DECIMAL(6,1) DEFAULT 0.0,
	raw_file_size_MB DECIMAL(10,1) DEFAULT 0.0,
	compressed BOOLEAN DEFAULT FALSE,
	edge BOOLEAN DEFAULT FALSE,
	write_to_tape BOOLEAN DEFAULT FALSE,
	delete_file BOOLEAN DEFAULT FALSE,
	restore_history VARCHAR(255) DEFAULT NULL,
	comments TEXT);''')

	print 'Table paperdata created'

	return None

def make_paperjunk(cursor):
	cursor.execute('''CREATE TABLE paperjunk (
	junk_path VARCHAR(100) DEFAULT NULL,
	folio_path VARCHAR(100) DEFAULT NULL,
	uv_path VARCHAR(100) DEFAULT NULL,
	junk_size_bytes BIGINT DEFAULT 0,
	usb_number INT DEFAULT 99,
	renamed tinyint(1) DEFAULT 0);''')

	print 'Table paperjunk created'

	return None

def make_paperrename(cursor):
	cursor.execute('''CREATE TABLE paperrename (
	raw_path VARCHAR(100) DEFAULT NULL,
	julian_day INT DEFAULT 0,
	actual_amount INT DEFAULT 0,
	expected_amount INT DEFAULT 288,
	moved tinyint(1) DEFAULT 0);''')

	print 'Table paperrename created'

	return None

def make_paperfeed(cursor):
	cursor.execute('''CREATE TABLE paperfeed (
	raw_path VARCHAR(100) DEFAULT NULL,
	julian_day INT DEFAULT 0,
	moved tinyint(1) DEFAULT 0);''')

	print 'Table paperfeed created'

	return None

if __name__ == '__main__':
	#inputs for user to access database
	usrnm = raw_input('Username: ')
	pswd = getpass.getpass('Password: ')
	if len(sys.argv) > 1:
		table = sys.argv[1]
	else:
		table = raw_input('Create which table (junk, rename, feed, data, all)? :')

	# open a database connection
	# be sure to change the host IP address, username, password and database name to match your own
	connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)

	# prepare a cursor object using cursor() method
	cursor = connection.cursor()

	if table == 'junk':
		make_paperjunk(cursor)
	elif table == 'rename':
		make_paperrename(cursor)
	elif table == 'feed':
		make_paperfeed(cursor)
	elif table == 'data':
		make_paperdata(cursor)
	elif table == 'all':
		make_paperjunk(cursor)
		make_paperrename(cursor)
		make_paperfeed(cursor)
		make_paperdata(cursor)

	# Close and Save database connection
	cursor.close()
	connection.commit()
	connection.close()
