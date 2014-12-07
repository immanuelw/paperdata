#!/usr/bin/python
# -*- coding: utf-8 -*-
# Update data in MySQL table 

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass
import os

### Script to check for all day data compression
### changes ready_to_tape field if all compressed

### Author: Immanuel Washington
### Date: 8-20-14

def tape_check(era, usrnm, pswd):
	#checks if files of the same Julian Date have all completed compression
	connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)

	# prepare a cursor object using cursor() method
	cursor = connection.cursor()

	#counting the amount of files in each day
	cursor.execute('''SELECT julian_day, COUNT(*) FROM paperdata WHERE era = %d and edge = 0 GROUP BY julian_day'''%(era))
	sec_results = cursor.fetchall()

	#counting the amount of compressed files in each day
	cursor.execute('''SELECT julian_day, COUNT(*), tape_index, raw_path FROM paperdata WHERE era = %d and compressed = 1 and raw_path != 'NULL' GROUP BY julian_day'''%(era))
	thr_results = cursor.fetchall()

	#create dictionary with julian_day as key, count as value
	res = {}
	for item in sec_results:
		res.update({item[0]:item[1]})

	#testing if same amount in each day, updating if so
	for item in thr_results:
		jday = int(item[0])
		if item[2] == 'NULL' and item[3] != 'ON TAPE':
			if res[item[0]] == item[1]:
				ready_to_tape = 1
				cursor.execute('''
				UPDATE paperdata
				SET write_to_tape = 1
				WHERE julian_day = %d
				'''%(jday))

	print 'Table data updated.'

	# close the cursor object
	cursor.close()
	connection.commit()
	connection.close()

	return None

if __name__ == '__main__':
	usrnm = raw_input('Root username: ')
	pswd = getpass.getpass('Root password: ')
	era = int(raw_input('32, 64, or 128?: '))
	tape_check(era, usrnm, pswd)
