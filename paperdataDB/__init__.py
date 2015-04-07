#!/usr/bin/python
# -*- coding: utf-8 -*-
# Module to allow easier finding of data in scripts 

### Author: Immanuel Washington
### Date: 02-03-14

# import the MySQLdb and sys modules
import MySQLdb
import MySQLdb.cursors
import sys
import getpass
import collections
import decimal

# Config variables

NOSEARCH = 0
SEARCH = 1

MIN = 2
MAX = 3
EXACT = 4
RANGE = 5
LIST = 6
NONE = 7

options = {EXACT:'EXACT', MIN:'MIN', MAX:'MAX', RANGE:'RANGE', LIST:'LIST', NONE:'NONE'}
classes = ('paperdata', 'paperjunk', 'paperrename', 'paperfeed')

# Function to create and output dictionary of results from query
def dbsearch_dict(query):
	# open a database connection
	# be sure to change the host IP address, username, password and database name to match your own
	connection = MySQLdb.connect (host = 'shredder', user = 'paperboy', passwd = 'paperboy', db = 'paperdata', local_infile=True)

	# prepare a cursor object using cursor() method
	cursor = connection.cursor(cursorclass = MySQLdb.cursors.DictCursor)

	# execute the SQL query using execute() method.
	cursor.execute('%s', (query,))
	
	#finds all rows outputted by query, prints them
	results = cursor.fetchall()

	#converts to dictionary
	results = list(results)

	result = collections.defaultdict(list)

	for dic in results:
		for key, value in dic.items():
			result(key).append(value)

	#complete
	print 'Query Complete'

	# Close connection to database
	cursor.close()
	connection.close()

	return result

# Function to create and output list of results from query
def dbsearch(query):
	# open a database connection
	# be sure to change the host IP address, username, password and database name to match your own
	connection = MySQLdb.connect (host = 'shredder', user = 'paperboy', passwd = 'paperboy', db = 'paperdata', local_infile=True)

	# prepare a cursor object using cursor() method
	cursor = connection.cursor()

	# execute the SQL query using execute() method.
	cursor.execute('%s', (query,))

	#finds all rows outputted by query, prints them
	results = cursor.fetchall()

	#complete
	print 'Query Complete'

	# Close connection to database
	cursor.close()
	connection.close()

	return results

class paperdata:
	def __init__(self):
		self.db_list = ('path',
						'era',
						'era_type',
						'obsnum',
						'md5sum',
						'julian_day',
						'julian_date',
						'polarization',
						'data_length',
						'raw_path',
						'cal_path',
						'npz_path',
						'final_product_path',
						'tape_index',
						'compr_file_size_MB',
						'raw_file_size_MB',
						'compressed',
						'edge',
						'write_to_tape',
						'delete_file',
						'restore_history',
						'comments')
		(self.PATH,
		self.ERA,
		self.ERA_TYPE,
		self.OBSNUM,
		self.MD5SUM,
		self.JDAY,
		self.JDATE,
		self.POL,
		self.LENGTH,
		self.RAW_PATH,
		self.CAL_PATH,
		self.NPZ_PATH,
		self.FINAL_PRODUCT_PATH,
		self.TAPE_INDEX,
		self.COMPR_SIZE,
		self.RAW_SIZE,
		self.COMPRESSED,
		self.EDGE,
		self.WRITE_TO_TAPE,
		self.DELETE_FILE,
		self.RESTORE_HISTORY,
		self.COMMENTS) = range(10, 10 + len(self.db_list))
		self.db_dict = {self.PATH:'path',
						self.ERA:'era',
						self.ERA_TYPE:'era_type',
						self.OBSNUM:'obsnum',
						self.MD5SUM:'md5sum',
						self.JDAY:'julian_day',
						self.JDATE:'julian_date',
						self.POL:'polarization',
						self.LENGTH:'data_length',
						self.RAW_PATH:'raw_path',
						self.CAL_PATH:'cal_path',
						self.NPZ_PATH:'npz_path',
						self.FINAL_PRODUCT_PATH:'final_product_path',
						self.TAPE_INDEX:'tape_index',
						self.COMPR_SIZE:'compr_file_size_MB',
						self.RAW_SIZE:'raw_file_size_MB',
						self.COMPRESSED:'compressed',
						self.EDGE:'edge',
						self.WRITE_TO_TAPE:'write_to_tape',	
						self.DELETE_FILE:'delete_file',
						self.RESTORE_HISTORY:'restore_history',
						self.COMMENTS:'comments'}
		self.db_inst = {'path':('VARCHAR(100)', 'NULL'),
						'era':('INT', '0'),
						'era_type':('VARCHAR(100)', 'NULL'),
						'obsnum':('BIGINT', '0'),
						'md5sum':('VARCHAR(100)', 'NULL'),
						'julian_day':('INT', '0'),
						'julian_date':('DECIMAL(12,5)', '0.00000'),
						'polarization':('VARCHAR(4)', 'all'),
						'data_length':('DECIMAL(6,5)', '0.00000'),
						'raw_path':('VARCHAR(100)', 'NULL'),
						'cal_path':('VARCHAR(100)', 'NULL'),
						'npz_path':('VARCHAR(100)', 'NULL'),
						'final_product_path':('VARCHAR(100)', 'NULL'),
						'tape_index':('VARCHAR(100)', 'NULL'),
						'compr_file_size_MB':('DECIMAL(7,2)', '0.00'),
						'raw_file_size_MB':('DECIMAL(7,2)', '0.00'),
						'compressed':('INT', '0'),
						'edge':('INT', '0'),
						'write_to_tape':('INT', '0'),
						'delete_file':('INT', '0'),
						'restore_history':('TEXT', ''),
						'comments':('TEXT', '')}
		self.db_descr = {'path':'path of compressed file',
						'era':'era of observation taken: 32, 64, 128',
						'era_type':'type of uv file: dual pol, etc.',
						'obsnum':'observation number used to track uv files using integer',
						'md5sum':'md5 checksum of raw uv file',
						'julian_day':'integer value showing the last 4 digits for any uv file to separate into days: ex:(2456601 -> 6601)',
						'julian_date':'Julian Date of uv file',
						'polarization':'polarization of uv file',
						'data_length':'length of time data was taken for particular uv file',
						'raw_path':'path of raw file',
						'cal_path':'path of calibration script',
						'npz_path':'path of .npz file used to track flags after compression',
						'final_product_path':'path of completed product (ex:uvcRREz?)',
						'tape_index':'index of raw file if it has been written to a tape',
						'compr_file_size_MB':'size of the compressed uv file in megabytes',
						'raw_file_size_MB':'size of the raw uv file in megabytes',
						'compressed':'boolean (0/1) value on whether compressed file exists and is logged in database',
						'edge':'boolean (0/1) value on whether raw uv file is on edge of day -- thus no compressed file can exist',
						'write_to_tape':'boolean (0/1) value on whether raw file can be written to tape',
						'delete_file':'boolean (0/1) value on whether raw file can be deleted',
						'restore_history':'history of times a file has been restored to folio from tape',
						'comments':'miscellaneous comments about uv file'}
		self.var_flo = ('julian_date',
						'data_length',
						'compr_file_size_MB',
						'raw_file_size_MB')
		self.var_str = ('path',
						'era_type',
						'md5sum',
						'polarization',
						'raw_path',
						'cal_path',
						'npz_path',
						'final_product_path',
						'tape_index',
						'restore_history',
						'comments')
		self.var_int = ('era',
						'obsnum',
						'julian_day',
						'compressed',
						'edge',
						'write_to_tape',
						'delete_file')
		self.table = 'paperdata'
		self.values = '%s' + ',%s' * (len(self.db_list) - 1)

class paperjunk:
	def __init__(self):
		self.db_list = ('junk_path',
						'folio_path',
						'uv_path',
						'junk_size_bytes',
						'usb_number',
						'renamed')
		(self.JUNK_PATH,
		self.FOLIO_PATH,
		self.UV_PATH,
		self.JUNK_SIZE_BYTES,
		self.USB_NUMBER,
		self.RENAMED) = range(10, 10 + len(self.db_list))
		self.db_dict = {self.JUNK_PATH:'junk_path',
						self.FOLIO_PATH:'folio_path',
						self.UV_PATH:'uv_path',
						self.JUNK_SIZE_BYTES:'junk_size_bytes',
						self.USB_NUMBER:'usb_number',
						self.RENAMED:'renamed'}
		self.db_inst = {'junk_path':('VARCHAR(100)', 'NULL'),
						'folio_path':('VARCHAR(100)', 'NULL'),
						'uv_path':('VARCHAR(100)', 'NULL'),
						'junk_size_bytes':('INT', '0'),
						'usb_number':('INT', '0'),
						'renamed':('TINYINT', '0')}
		self.db_descr = {'junk_path':'path of unnamed visdata file to be reconstructed into uv file',
						'folio_path':'path of unnamed visdata file to be reconstructed into uv file as it exists on folio',
						'uv_path':'path of named reconstructed uv file',
						'junk_size_bytes':'size of unnamed visdata in bytes',
						'usb_number':'Number of Kroll USB as labeled',
						'renamed':'boolean (0/1) value on whether file has been put into named uv file'}
		self.var_flo = ()
		self.var_str = ('junk_path',
						'folio_path',
						'uv_path')
		self.var_int = ('junk_size_bytes',
						'usb_number',
						'renamed')
		self.table = 'paperjunk'
		self.values = '%s' + ',%s' * (len(self.db_list) - 1)

class paperrename:
	def __init__(self):
		self.db_list = ('raw_path',
						'julian_day',
						'actual_amount',
						'expected_amount',
						'moved')
		(self.RAW_PATH,
		self.JULIAN_DAY,
		self.ACTUAL_AMOUNT,
		self.EXPECTED_AMOUNT,
		self.MOVED) = range(10, 10 + len(self.db_list))
		self.db_dict = {self.RAW_PATH:'raw_path',
						self.JULIAN_DAY:'julian_day',
						self.ACTUAL_AMOUNT:'actual_amount',
						self.EXPECTED_AMOUNT:'expected_amount',
						self.MOVED:'moved'}
		self.db_inst = {'raw_path':('VARCHAR(100)', 'NULL'),
						'julian_day':('INT', '0'),
						'actual_amount':('INT', '0'),
						'expected_amount':('INT', '288'),
						'moved':('TINYINT', '0')}
		self.db_descr = {'raw_path':'raw_path of file to be sent to paperfeed',
						'julian_day':'Julian Day of observation',
						'actual_amount':'Amount of uv files in this day logged in the paperrename table',
						'expected_amount':'Amount of uv files expected for this specific Julian Day -- used to limit which days are compressed',
						'moved':'boolean (0/1) value on whether file has been moved to be paperfeed'}
		self.var_flo = ()
		self.var_str = ('raw_path',)
		self.var_int = ('julian_day',
						'actual_amount',
						'expected_amount',
						'moved')
		self.table = 'paperrename'
		self.values = '%s' + ',%s' * (len(self.db_list) - 1)

class paperfeed:
	def __init__(self):
		self.db_list = ('raw_path',
						'julian_day',
						'moved')
		(self.RAW_PATH,
		self.JULIAN_DAY,
		self.MOVED) = range(10, 10 + len(self.db_list))
		self.db_dict = {self.RAW_PATH:'raw_path', self.JULIAN_DAY:'julian_day', self.MOVED:'moved'}
		self.db_inst = {'raw_path':('VARCHAR(100)', 'NULL'),
						'julian_day':('INT', '0'),
						'moved':('TINYINT', '0')}
		self.db_descr = {'raw_path':'raw_path of file to be sent to paperdistiller',
						'julian_day':'Julian Day of observation',
						'moved':'boolean (0/1) value on whether file has been moved to be compressed'}
		self.var_flo = ()
		self.var_str = ('raw_path',)
		self.var_int = ('julian_day',
						'moved')
		self.table = 'paperfeed'
		self.values = '%s' + ',%s' * (len(self.db_list) - 1)

#dictionary of instantiated classes
instant_class = {'paperdata':paperdata(), 'paperjunk':paperjunk(), 'paperrename':paperrename(), 'paperfeed':paperfeed()}
all_classes = (paperdata(), paperjunk(), paperrename(), paperfeed())

# Generate strings to load into query 
def fetch(info_list, db_dict, var_flo, var_str, var_int, table):
	# Instantiate variables to use to generate query string
	query = ()
	searchstr = ()

	# Info list should be ((field_name, search_field, option, (more_info)), 
	# Ex: (('paperfeed', NOSEARCH, EXACT, (2,)), ('user_perc', SEARCH, NONE, ()), ('intr_s', SEARCH, RANGE, (922, 935)))
	for item in info_list:
		if len(item) != 4:
			print 'ERROR -- LIST %s does not have enough entries' %(item)
			return None #HOW SHOULD I THROW ERRORS?

		# Instantiates field variable
		if isinstance(item(0), str):
			field = item(0)
		elif isinstance(item(0), int):
			field = db_dict(item(0))

		if item(2) == EXACT:
			if len(item(3)) != 1:
				print 'ERROR -- LIST %s does not have the right amount of entries' %(item)
				return None #HOW SHOULD I THROW ERRORS?
	
			exact = item(3)(0)

			# Adding info to lists to generate strings later
			if item(1) == SEARCH:
				query.append(field)
			if field in var_flo:
				searchstr.append('%s = %.6f'%(field, exact))
			elif field in var_str:
				searchstr.append("%s = '%s'"%(field, exact))
			elif field in var_int:
				searchstr.append('%s = %d'%(field, exact))

		elif item(2) == MIN:
			if len(item(3)) != 1:
				print 'ERROR -- LIST %s does not have the right amount of entries' %(item)
				return None #HOW SHOULD I THROW ERRORS?

			min = item(3)(0)

			# Adding info to lists to generate strings later
			if item(1) == SEARCH:
				query.append(field)
			if field in var_flo:
				searchstr.append('%s >= %.6f'%(field, min))
			elif field in var_str:
				searchstr.append("%s >= '%s'"%(field, min))
			elif field in var_int:
				searchstr.append('%s >= %d'%(field, min))

		elif item(2) == MAX:
			if len(item(3)) != 1:
				print 'ERROR -- LIST %s does not have the right amount of entries' %(item) 
				return None #HOW SHOULD I THROW ERRORS?

			max = item(3)(0)

			# Adding info to lists to generate strings later
			if item(1) == SEARCH:
				query.append(field)
			if field in var_flo:
				searchstr.append('%s <= %.6f'%(field, max))
			elif field in var_str:
				searchstr.append("%s <= '%s'"%(field, max))
			elif field in var_int:
				searchstr.append('%s <= %d'%(field, max))

		elif item(2) == RANGE:
			if len(item(3)) != 2:
				print 'ERROR -- LIST %s does not have the right amount of entries' %(item) 
				return None #HOW SHOULD I THROW ERRORS?

			min = item(3)(0)
			max = item(3)(1)

			# Adding info to lists to generate strings later
			if item(1) == SEARCH:
				query.append(field)
			if field in var_flo:
				searchstr.append('%s >= %.6f and %s <= %.6f'%(field, min, field, max))
			elif field in var_str:
				searchstr.append("%s >= '%s' and %s <= '%s'"%(field, min, field, max))
			elif field in var_int:
				searchstr.append('%s >= %d and %s <= %d'%(field, min, field, max))

		elif item(2) == LIST:
			if len(item(3)) <= 1:
				print 'ERROR -- LIST %s does not have the right amount of entries' %(item)
				return None #HOW SHOULD I THROW ERRORS?

			if item(1) == SEARCH:
				query.append(field)
			if field in var_flo:
				for it in item(3):
					if it == item(3)(0):
						list_str = '(%s = %.6f' %(field, it)
					elif it == item(3)(-1):
						list_str = list_str + ' or %s = %.6f)' %(field, it)
					else:
						list_str = list_str + ' or %s = %.6f' %(field, it)
			elif field in var_str:
				for it in item(3):
					if it == item(3)(0):
						list_str = "(%s = '%s'" %(field, it)
					elif it == item(3)(-1):
						list_str = list_str + " or %s = '%s')" %(field, it)
					else:
						list_str = list_str + " or %s = '%s'" %(field, it)
			elif field in var_int:
				for it in item(3):
					if it == item(3)(0):
						list_str = '(%s = %d' %(field, it)
					elif it == item(3)(-1):
						list_str = list_str + ' or %s = %d)' %(field, it)
					else:
						list_str = list_str + ' or %s = %d' %(field, it)
			searchstr.append(list_str)

		elif item(2) == NONE:
			if len(item(3)) != 0:
				print 'ERROR -- LIST %s has too many entries' %(item) 
				return None #HOW SHOULD I THROW ERRORS?

			# Adding info to lists to generate strings later
			if item(1) == SEARCH:
				query.append(field)

		else:
			return None #HOW SHOULD I THROW ERRORS?

	for item in searchstr:
		if item == searchstr(0):
			qsearch = item
		else:
			qsearch = qsearch + ' and ' + item

	for item in query:
		if len(query) == 1:
			if qsearch == '':
				dbstr = 'SELECT ' + item + ' FROM ' + table
			else:
				dbstr = 'SELECT ' + item + ' FROM ' + table + ' WHERE ' + qsearch
		else:
			if item == query(0):
				dbstr = 'SELECT ' + item + ', '
			elif item == query(-1):
				dbstr = dbstr + item + ' FROM ' + table + ' WHERE ' + qsearch
			else:
				dbstr = dbstr + item + ', '

	return dbstr

# Near redundant class instantiation in order to facilitate easy repeatable access to paperdata through scripts
# It's a way to code in queries without using the GUI whenever a change is wanted
# REQUIRES knowledge of fields and paperdataDB constants

class searchobj:
	def __init__(self, db_dict, var_flo, var_str, var_int, table):
		self.info_list = ()
		self.db_dict = db_dict
		self.var_flo = var_flo
		self.var_str = var_str
		self.var_int = var_int
		self.table = table
	def add_to_output(self, field, search_bool, limit, *range):
		self.info_list.append((field, search_bool, limit, range))
		return None
	def output(self):
		return dbsearch(fetch(self.info_list, self.db_dict, self.var_flo, self.var_str, self.var_int, self.db))
	def output_dict(self):
		return dbsearch_dict(fetch(self.info_list, self.db_dict, self.var_flo, self.var_str, self.var_int, self.db))

#Only do things if running this script, not importing
if __name__ == '__main__':
	print 'Not a script file, just a module'
