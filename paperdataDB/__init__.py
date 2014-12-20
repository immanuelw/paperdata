#!/usr/bin/python
# -*- coding: utf-8 -*-
# Module to allow easier finding of data in scripts 

### Author: Immanuel Washington
### Date: 10-12-14

# import the MySQLdb and sys modules
import MySQLdb
import MySQLdb.cursors
import sys
import getpass
import collections
import decimal

#Input list which indicates certain aspects fo the query

#for files do
# import paperdataDB as pdb
# files = pdb.dbsearch(pdb.fetch(list_of_info), pswd)
#OR combine dbsearch to include both
# files = pdb.dbsearch(db_query, pswd)

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
NPZ_PATH = 11
FINAL_PRODUCT_PATH = 12
TAPE_INDEX = 13
COMPR_SIZE = 14
RAW_SIZE = 15
COMPRESSED = 16
EDGE = 17
WRITE_TO_TAPE = 18
DELETE_FILE = 19
RESTORE_HISTORY = 20
COMMENTS = 21

MIN = 25
MAX = 26
EXACT = 27 
RANGE = 28
LIST = 29
NONE = 30

SEARCH = 1
NOSEARCH = 0

def constants():
	const = {PATH:'PATH', ERA:'ERA', ERA_TYPE:'ERA_TYPE', OBSNUM:'OBSNUM', MD5SUM:'MS5SUM', JDAY:'JDAY', JDATE:'JDATE', POL:'POL', LENGTH:'LENGTH', RAW_PATH:'RAW_PATH', CAL_PATH:'CAL_PATH', NPZ_PATH:'NPZ_PATH', FINAL_PRODUCT_PATH: 'FINAL PRODUCT_PATH', TAPE_INDEX:'TAPE_INDEX', COMPR_SIZE:'COMPR_SIZE', RAW_SIZE:'RAW_SIZE', COMPRESSED:'COMPRESSED', WRITE_TO_TAPE:'WRITE_TO_TAPE', DELETE_FILE:'DELETE_FILE', RESTORE_HISTORY:'RESTORE_HISTORY', COMMENTS:'COMMENTS', MIN:'MIN', MAX:'MAX', EXACT:'EXACT', RANGE:'RANGE', NONE:'NONE', SEARCH:'SEARCH', NOSEARCH:'NOSEARCH'}
	return const

pd_dict = {PATH:'path', ERA:'era', ERA_TYPE:'era_type', OBSNUM:'obsnum', MD5SUM:'md5sum', JDAY:'julian_day', JDATE:'julian_date', POL:'polarization', LENGTH:'data_length', RAW_PATH:'raw_path', CAL_PATH:'cal_path', NPZ_PATH:'npz_path', FINAL_PRODUCT_PATH:'final_product_path', TAPE_INDEX:'tape_index', COMPR_SIZE:'compr_file_size_MB', RAW_SIZE:'raw_file_size_MB', COMPRESSED:'compressed', WRITE_TO_TAPE:'write_to_tape', DELETE_FILE:'delete_file', RESTORE_HISTORY:'restore_history', COMMENTS:'comments'}

def fields():
	field_list = ['path', 'era', 'era_type', 'obsnum', 'md5sum', 'julian_day', 'julian_date', 'polarization', 'data_length', 'raw_path', 'cal_path', 'npz_path', 'final_product_path', 'tape_index', 'compr_file_size_MB', 'raw_file_size_MB', 'compressed', 'write_to_tape', 'delete_file', 'restore_history', 'comments']

	return field_list

def dict():
	paperdata_dict = {PATH:'path', ERA:'era', ERA_TYPE:'era_type', OBSNUM:'obsnum', MD5SUM:'md5sum', JDAY:'julian_day', JDATE:'julian_date', POL:'polarization', LENGTH:'data_length', RAW_PATH:'raw_path', CAL_PATH:'cal_path', NPZ_PATH:'npz_path', FINAL_PRODUCT_PATH:'final_product_path', TAPE_INDEX:'tape_index', COMPR_SIZE:'compr_file_size_MB', RAW_SIZE:'raw_file_size_MB', COMPRESSED:'compressed', WRITE_TO_TAPE:'write_to_tape', DELETE_FILE:'delete_file', RESTORE_HISTORY:'restore_history', COMMENTS:'comments'}
	return paperdata_dict

def options():
	opt = {EXACT:'EXACT', MIN:'MIN', MAX:'MAX', RANGE:'RANGE', LIST:'LIST', NONE:'NONE'}
	return opt

def dbsearch_dict(query):
	# open a database connection
	# be sure to change the host IP address, username, password and database name to match your own
	connection = MySQLdb.connect (host = 'shredder', user = 'paperboy', passwd = 'paperboy', db = 'paperdata', local_infile=True)

	# prepare a cursor object using cursor() method
	cursor = connection.cursor(cursorclass = MySQLdb.cursors.DictCursor)

	# execute the SQL query using execute() method.
	cursor.execute(query)
	
	#finds all rows outputted by query, prints them
	results = cursor.fetchall()

	#converts to dictionary
	results = list(results)

	result = collections.defaultdict(list)

	for dic in results:
		for key, value in dic.items():
			result[key].append(value)

	#complete
	print 'Query Complete'

	# Close connection to database
	cursor.close()
	connection.close()

	return result

def dbsearch(query):
	# open a database connection
	# be sure to change the host IP address, username, password and database name to match your own
	connection = MySQLdb.connect (host = 'shredder', user = 'paperboy', passwd = 'paperboy', db = 'paperdata', local_infile=True)

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
			return None #HOW SHOULD I THROW ERRORS?

		#instantiates field variable
		if isinstance(item[0], str):
			field = item[0]
		elif isinstance(item[0], int):
			field = pd_dict[item[0]]

		if item[2] == EXACT:
			if len(item[3]) != 1:
				print 'ERROR -- LIST %s does not have the right amount of entries' %(item)
				return None #HOW SHOULD I THROW ERRORS?

			exact = item[3][0]

			#adding info to lists to generate strings later
			if item[1] == SEARCH:
				query.append(field)
			if field == 'julian_date':
				searchstr.append('%s = %.5f'%(field, exact))
			elif field == 'polarization':
				searchstr.append("%s = '%s'"%(field, exact))
			else:
				searchstr.append('%s = %d'%(field, exact))

		elif item[2] == MIN:
			if len(item[3]) != 1:
				print 'ERROR -- LIST %s does not have the right amount of entries' %(item)
				return None #HOW SHOULD I THROW ERRORS?

			min = item[3][0]

			#adding info to lists to generate strings later
			if item[1] == SEARCH:
				query.append(field)
			if field == 'julian_date':			
				searchstr.append('%s >= %.5f'%(field, min))
			else:
				searchstr.append('%s >= %d'%(field, min))

		elif item[2] == MAX:
			if len(item[3]) != 1:
				print 'ERROR -- LIST %s does not have the right amount of entries' %(item) 
				return None #HOW SHOULD I THROW ERRORS?

			max = item[3][0]

			#adding info to lists to generate strings later
			if item[1] == SEARCH:
				query.append(field)
			if field == 'julian_date':
				searchstr.append('%s <= %.5f'%(field, max))
			else:
				searchstr.append('%s <= %d'%(field, max))

		elif item[2] == RANGE:
			if len(item[3]) != 2:
				print 'ERROR -- LIST %s does not have the right amount of entries' %(item) 
				return None #HOW SHOULD I THROW ERRORS?

			min = item[3][0]
			max = item[3][1]

			#adding info to lists to generate strings later
			if item[1] == SEARCH:
				query.append(field)
			if field == 'julian_date':
				searchstr.append('%s >= %.5f and %s <= %.5f'%(field, min, field, max))

			else:
				searchstr.append('%s >= %d and %s <= %d'%(field, min, field, max))

		elif item[2] == LIST:
			if len(item[3]) <= 1:
				print 'ERROR -- LIST %s does not have the right amount of entries' %(item)
				return None #HOW SHOULD I THROW ERRORS?

			if item[1] == SEARCH:
				query.append(field)
			if field == 'julian_date':
				for it in item[3]:
					if it == item[3][0]:
						list_str = '(%s = %.5f' %(field, it)
					elif it == item[3][-1]:
						list_str = list_str + ' or %s = %.5f)' %(field, it)
					else:
						list_str = list_str + ' or %s = %.5f' %(field, it)
			elif field == 'polarization':
				for it in item[3]:
					if it == item[3][0]:
						list_str = "(%s = '%s'" %(field, it)
					elif it == item[3][-1]:
						list_str = list_str + " or %s = '%s')" %(field, it)
					else:
						list_str = list_str + " or %s = '%s'" %(field, it)
			else:
				for it in item[3]:
					if it == item[3][0]:
						list_str = '(%s = %d' %(field, it)
					elif it == item[3][-1]:
						list_str = list_str + ' or %s = %d)' %(field, it)
					else:
						list_str = list_str + ' or %s = %d' %(field, it)
			searchstr.append(list_str)

		elif item[2] == NONE:
			if len(item[3]) != 0:
				print 'ERROR -- LIST %s has too many entries' %(item) 
				return None #HOW SHOULD I THROW ERRORS?

			#adding info to lists to generate strings later
			if item[1] == SEARCH:
				query.append(field)

		else:
			return None #HOW SHOULD I THROW ERRORS?

	for item in searchstr:
		if item == searchstr[0]:
			qsearch = item
		else:
			qsearch = qsearch + ' and ' + item

	for item in query:
		if len(query) == 1:
			if qsearch == '':
				dbstr = 'SELECT ' + item + ' FROM paperdata'
			else:
				dbstr = 'SELECT ' + item + ' FROM paperdata WHERE ' + qsearch
		else:
			if item == query[0]:
				dbstr = 'SELECT ' + item + ', '
			elif item == query[-1]:
				dbstr = dbstr + item + ' FROM paperdata WHERE ' + qsearch
			else:
				dbstr = dbstr + item + ', '

	return dbstr

class searchobj:
	def __init__(self):
		self.info_list = []
		return None
	def return(self, field, search_bool, limit, *range):
		self.info_list.append([field, search_bool, limit, range])
		return None
	def output(self):
		return dbsearch(fetch(self.info_list))
	def output_dict(self):
		return dbsearch_dict(fetch(self.info_list))

#Only do things if running this script, not importing
if __name__ == '__main__':
	print 'Not a script file'
