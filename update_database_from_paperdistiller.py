#!/usr/bin/python
# -*- coding: utf-8 -*-
# Update data in MySQL table 

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass

table = 'paperdata'
usrnm = raw_input('Root username: ')
pswd = getpass.getpass('Root password: ')

compressed = 'compressed'
obsnum_string = 'obsnum'
tape = 'ready_to_tape'
j_day = 'julian_day'

res = {}

#checks if files of the same Julian Date have all completed compression
#need way to get compr_value and obsnum from paperdistiller 

# open a database connection
connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdistiller', local_infile=True)

cursor = connection.cursor()

# execute the SQL query using execute() method.
cursor.execute('SELECT obsnum, status from observation order by julian_date')

#collects information from query
results = cursor.fetchall()

zer_results = {}

for item in results:
	zer_results.update({item[0]:item[1]})

#close, save and end connection
cursor.close()
connection.commit()
connection.close()


# open a database connection
# be sure to change the host IP address, username, password and database name to match your own
connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)

# prepare a cursor object using cursor() method
cursor = connection.cursor()

#set value to compressed files

cursor.execute('SELECT obsnum, path, tape_location, raw_location FROM paperdata WHERE era = 128 ORDER BY julian_date')
fir_results = cursor.fetchall()

#check if compressed file exists, if so set compr_value = 1
for item in fir_results:
	if item[2] == 'NULL' and item[3] != 'NULL':
		obsnum = item[0]
		if zer_results[item[0]] == 'COMPLETE':
			compr_value = 1
		elif os.path.isdir(item[1]):
			compr_value = 1
		else:
			compr_value = 0
		cursor.execute('''
		UPDATE %s
		SET %s = %d
		WHERE %s = %d;
		'''%(table, compressed, compr_value, obsnum_string, obsnum))

#counting the amount of files in each day
cursor.execute('SELECT julian_day, COUNT(*) FROM paperdata WHERE era = 128 GROUP BY julian_day')
sec_results = cursor.fetchall()

#counting the amount of compressed files in each day
cursor.execute('SELECT julian_day, COUNT(*), tape_location FROM paperdata WHERE era = 128 and compressed = 1 GROUP BY julian_day')
thr_results = cursor.fetchall()

#create dictionary with julian_day as key, count as value
for item in sec_results:
	res.update({item[0]:item[1]})

#testing if same amount in each day, updating if so
for item in thr_results:
	j_value = item[0]
	if item[2] == 'NULL':
		if res[item[0]] == item[1]:
			ready_to_tape = 1
			cursor.execute('''
	                UPDATE %s
	                SET %s = %d
	                WHERE %s = %d;
	                '''%(table, tape, ready_to_tape, j_day, j_value))

print 'Table data updated.'

# close the cursor object
cursor.close()

#save changes to database
connection.commit()

# close the connection
connection.close()

# exit the program
sys.exit()
