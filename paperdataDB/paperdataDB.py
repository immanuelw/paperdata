#!/usr/bin/python
# -*- coding: utf-8 -*-
# Module to allow easier finding of data in scripts 

### Author: Immanuel Washington
### Date: 10-12-14

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass

#Input list which indicates certain aspects fo the query

#for files do
# import paperdataDB.paperdataDB as pdb
#files = pdb.dbsearch(pdb.fetch(list_of_info), pswd)
#OR combine dbsearch to include both
#files = pdf.dbsearch(list_of_info, pswd)

# config variables
PATH = 0
ERA = 1
ERA_TYPE = 2
OBSNUM = 3
MD5SUM = 4
JDAY = 5
JDATE = 6
POL = 7
LENGTH = 8
RAW_PATH = 9
CAL_PATH = 10
TAPE_PATH = 11
COMPR_SIZE= 12
RAW_SIZE= 13
COMPRESSED= 14
READY_TO_TAPE= 15
DELETE_FILE = 16
RESTORE= 17

MIN = 20
MAX = 21
EXACT = 22 
RANGE = 23
NONE = 24

SEARCH = 30
NOSEARCH = 31

pd_dict = {PATH:'path', ERA:'era', ERA_TYPE:'era_type', OBSNUM:'obsnum', MD5SUM:'md5sum', JDAY:'julian_day', JDATE:'julian_date', POL:'polarization', LENGTH:'data_length', RAW_PATH:'raw_location', CAL_PATH:'cal_location', TAPE_PATH:'tape_location', COMPR_SIZE:'file_size_MB', RAW_SIZE:'raw_file_size_MB', COMPRESSED:'compressed', READY_TO_TAPE:'ready_to_tape', DELETE_FILE:'delete_file', RESTORE:'restore_history'}

def fields():
	field_list = ['path', 'era', 'era_type', 'obsnum', 'md5sum', 'julian_day', 'julian_date', 'polarization', 'data_length', 'raw_location', 'cal_location', 'tape_location', 'file_size_MB', 'raw_file_size_MB', 'compressed', 'ready_to_tape', 'delete_file', 'restore_history']

	return field_list

def dict():
	paperdata_dict = {PATH:'path', ERA:'era', ERA_TYPE:'era_type', OBSNUM:'obsnum', MD5SUM:'md5sum', JDAY:'julian_day', JDATE:'julian_date', POL:'polarization', LENGTH:'data_length', RAW_PATH:'raw_location', CAL_PATH:'cal_location', TAPE_PATH:'tape_location', COMPR_SIZE:'file_size_MB', RAW_SIZE:'raw_file_size_MB', COMPRESSED:'compressed', READY_TO_TAPE:'ready_to_tape', DELETE_FILE:'delete_file', RESTORE:'restore_history'}
	return paperdata_dict

def options():
	opt = ['exact', 'min', 'max', 'range', 'none']
	return opt

def dbsearch(query, pswd):
	# open a database connection
	# be sure to change the host IP address, username, password and database name to match your own
	connection = MySQLdb.connect (host = 'shredder', user = 'immwa', passwd = pswd, db = 'paperdata', local_infile=True)

	# prepare a cursor object using cursor() method
	cursor = connection.cursor()

	# execute the SQL query using execute() method.
	cursor.execute(query)

	#finds all rows outputted by query, prints them
	results = cursor.fetchall()
	#complete
	print 'Query Complete'

	# Close connection to database
	cursor.close()
	connection.close()

	return results

#Generate strings to load into query 
def fetch(info_list):
	#instantiate variables to use to generate query string
	query = []
	searchstr = []

	#info list should be [[field_name, search_field, option, [more_info]], 
	#ex: [['era', NOSEARCH, EXACT, [32]], ['path', SEARCH, NONE, []], ['julian_day', SEARCH, RANGE, [922, 935]]]
	for item in info_list:
		if len(item) != 4:
			print 'ERROR -- LIST %s does not have enough entries' %(item)
			sys.exit() #HOW SHOULD I THROW ERRORS?

		#instantiates field variable
		field = item[0]

		if item[2] == EXACT:
			if len(item[3]) != 1:
				print 'ERROR -- LIST %s does not have the right amount of entries' %(item)
				sys.exit() #HOW SHOULD I THROW ERRORS?

			exact = item[3][0]

			#adding info to lists to generate strings later
			if item[1] == SEARCH:
				query.append(field)
			searchstr.append('%s = %d'%(field, exact))

		elif item[2] == MIN:
			if len(item[3]) != 1:
				print 'ERROR -- LIST %s does not have the right amount of entries' %(item)
				sys.exit() #HOW SHOULD I THROW ERRORS?

			min = item[3][0]

			#adding info to lists to generate strings later
			if item[1] == SEARCH:
				query.append(field)
			searchstr.append('%s >= %d'%(field, min))

		elif item[2] == MAX:
			if len(item[3]) != 1:
				print 'ERROR -- LIST %s does not have the right amount of entries' %(item) 
				sys.exit() #HOW SHOULD I THROW ERRORS?

			max = item[3][0]

			#adding info to lists to generate strings later
			if item[1] == SEARCH:
				query.append(field)
			searchstr.append('%s <= %d'%(field, max))

		elif item[2] == RANGE:
			if len(item[3]) != 2:
				print 'ERROR -- LIST %s does not have the right amount of entries' %(item) 
				sys.exit() #HOW SHOULD I THROW ERRORS?

			min = item[3][0]
			max = item[3][1]

			#adding info to lists to generate strings later
			if item[1] == SEARCH:
				query.append(field)
			searchstr.append('%s >= %d and %s <= %d'%(field, min, field, max))

		elif item[2] == NONE:
			if len(item[3]) != 0:
				print 'ERROR -- LIST %s has too many entries' %(item) 
				sys.exit() #HOW SHOULD I THROW ERRORS?

			#adding info to lists to generate strings later
			if item[1] == SEARCH:
				query.append(field)

		else:
			sys.exit() #HOW SHOULD I THROW ERRORS?

	for item in searchstr:
		if item == searchstr[0]:
			qsearch = item
		else:
			qsearch = qsearch + ' and ' + item

	for item in query:
		if len(query) == 1:
			dbstr = 'SELECT ' + item + ' FROM paperdata WHERE ' + qsearch
		else:
			if item == query[0]:
				dbstr = 'SELECT ' + item + ', '
			elif item == query[-1]:
				dbstr = dbstr + item + ' FROM paperdata WHERE ' + qsearch
			else:
				dbstr = dbstr + item + ', '

	return dbstr

#Only do things if running this script, not importing
if __name__ == '__main__':
	#allows user to input database and table queried
	usrnm = raw_input('Username: ')
	pswd = getpass.getpass('Password: ')
