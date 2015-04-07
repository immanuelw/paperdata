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
classes = ('monitor_files', 'ram', 'iostat', 'cpu')

# Function to create and output dictionary of results from query
def dbsearch_dict(query):
	# open a database connection
	# be sure to change the host IP address, username, password and database name to match your own
	connection = MySQLdb.connect (host = 'shredder', user = 'immwa', passwd = 'immwa3978', db = 'ganglia', local_infile=True)

	# prepare a cursor object using cursor() method
	cursor = connection.cursor(cursorclass = MySQLdb.cursors.DictCursor)

	# execute the SQL query using execute() method.
	cursor.execute('''%s''', (query,))
	
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
	connection = MySQLdb.connect (host = 'shredder', user = 'immwa', passwd = 'immwa3978', db = 'ganglia', local_infile=True)

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

class monitor_files:
	def __init__(self):
		self.db_list = ('filename',
						'status',
						'del_time',
						'time_start',
						'time_end',
						'still_host',
						'time_date')
		(self.FILENAME,
		self.STATUS,
		self.DEL_TIME,
		self.TIME_START,
		self.TIME_END,
		self.STILL_HOST,
		self.TIME_DATE) = range(10, 10 + len(self.db_list))
		self.db_dict = {self.FILENAME:'filename',
						self.STATUS:'status',
						self.DEL_TIME:'del_time',
						self.TIME_START:'time_start',
						self.TIME_END:'time_end',
						self.STILL_HOST:'still_host',
						self.TIME_DATE:'time_date'}
		self.db_inst = {'filename':('VARCHAR(255)', 'NULL'),
						'status':('VARCHAR(255)', 'NULL'),
						'del_time':('BIGINT', '0'),
						'time_start':('BIGINT', '0'),
						'time_end':('BIGINT', '0'),
						'still_host'('VARCHAR(255)', 'NULL'),
						'time_date':('DECIMAL(13,6)', '0.000000')}
		self.db_descr = {'filename':'name/path of uv file being compressed',
						'status':'state of compression file is currently doing',
						'del_time':'time taken to finish step',
						'time_start':'time process started as a integer',
						'time_end':'time process ended as an integer',
						'still_host':'node that uv file is being compressed on',
						'time_date':'time and date entry was updated'}
		self.var_flo = ('time_date',)
		self.var_str = ('filename',
						'status',
						'still_host')
		self.var_int = ('del_time',
						'time_start',
						'time_end')
		self.table = 'monitor_files'
		self.values = '%s' + ',%s' * (len(self.db_list) - 1)

class ram:
	def __init__(self):
		self.db_list = ('host',
						'total',
						'used',
						'free',
						'shared',
						'buffers',
						'cached',
						'bc_used',
						'bc_free',
						'swap_total',
						'swap_used',
						'swap_free',
						'time_date')
		(self.HOST,
		self.TOTAL,
		self.USED,
		self.FREE,
		self.SHARED,
		self.BUFFERS,
		self.CACHED,
		self.BC_USED,
		self.BC_FREE,
		self.SWAP_TOTAL,
		self.SWAP_USED,
		self.SWAP_FREE,
		self.TIME_DATE) = range(10, 10 + len(self.db_list))
		self.db_dict = {self.HOST:'host',
						self.TOTAL:'total',
						self.USED:'used',
						self.FREE:'free',
						self.SHARED:'shared',
						self.BUFFERS:'buffers',
						self.CACHED:'cached',
						self.BC_USED:'bc_used',
						self.BC_FREE:'bc_free',
						self.SWAP_TOTAL:'swap_total',
						self.SWAP_TOTAL:'swap_used',
						self.SWAP_FREE:'swap_free',
						self.TIME_DATE:'time_date'}
		self.db_inst = {'host':('VARCHAR(255)', 'NULL'),
						'total':('BIGINT', '0'),
						'used':('BIGINT', '0'),
						'free':('BIGINT', '0'),
						'shared':('BIGINT', '0'),
						'buffers':('BIGINT', '0'),
						'cached':('BIGINT', '0'),
						'bc_used':('BIGINT', '0'),
						'bc_free':('BIGINT', '0'),
						'swap_total':('BIGINT', '0'),
						'swap_used':('BIGINT', '0'),
						'swap_free':('BIGINT', '0'),
						'time_date':('DECIMAL(13,6)', '0.000000')}
		self.db_descr = {'host':'system that is being monitored',
						'total':'total ram',
						'used':'used ram',
						'free':'free ram',
						'shared':'shared ram',
						'buffers':'buffers',
						'cached':'cached ram',
						'bc_used':'',
						'bc_free':'',
						'swap_total':'',
						'swap_used':'',
						'swap_free':'',
						'time_date':'time and date entry was updated'}
		self.var_flo = ('time_date',)
		self.var_str = ('host',)
		self.var_int = ('total',
						'used',
						'free',
						'shared',
						'buffers',
						'cached',
						'bc_used',
						'bc_free',
						'swap_total',
						'swap_used',
						'swap_free')
		self.table = 'ram'
		self.values = '%s' + ',%s' * (len(self.db_list) - 1)

class iostat:
	def __init__(self):
		self.db_list = ('host',
						'device',
						'tps',
						'read_s',
						'write_s',
						'bl_reads',
						'bl_writes',
						'time_date')
		(self.HOST,
		self.DEVICE,
		self.TPS,
		self.READ_S,
		self.WRITE_S,
		self.READS,
		self.WRITES,
		self.TIME_DATE) = range(10, 10 + len(self.db_list))
		self.db_dict = {self.HOST:'host',
						self.DEVICE:'device',
						self.TPS:'tps',
						self.READ_S:'read_s',
						self.WRITE_S:'write_s',
						self.READS:'bl_reads',
						self.WRITES:'bl_writes',
						self.TIME_DATE:'time_date'}
		self.db_inst = {'host':('VARCHAR(255)', 'NULL'),
						'device':('VARCHAR(100)', 'NULL'),
						'tps':('DECIMAL(7,2)', '0.00'),
						'read_s':('DECIMAL(7,2)', '0.00'),
						'write_s':('DECIMAL(7,2)', '0.00'),
						'bl_reads':('BIGINT' '0'),
						'bl_writes':('BIGINT' '0'),
						'time_date':('DECIMAL(13,6)', '0.000000')}
		self.db_descr = {'host':'system that is being monitored',
						'device':'',
						'tps':'',
						'read_s':'reads per second',
						'write_s':'writes per second',
						'bl_reads':'block reads',
						'bl_writes':'block writes',
						'time_date':'time and date entry was updated'}
		self.var_flo = ('tps',
						'read_s',
						'write_s',
						'time_date')
		self.var_str = ('host',
						'device')
		self.var_int = ('bl_reads',
						'bl_writes')
		self.table = 'iostat'
		self.values = '%s' + ',%s' * (len(self.db_list) - 1)

class cpu:
	def __init__(self):
		self.db_list = ('host',
						'cpu',
						'user_perc',
						'sys_perc',
						'iowait_perc',
						'idle_perc',
						'intr_s',
						'time_date')
		(self.HOST,
		self.CPU,
		self.USER_PERC,
		self.SYS_PERC,
		self.IOWAIT_PERC,
		self.IDLE_PERC,
		self.INTR_S,
		self.TIME_DATE) = range(10, 10 + len(self.db_list))
		self.db_dict = {self.HOST:'host',
						self.CPU:'cpu',
						self.USER_PERC:'user_perc',
						self.SYS_PERC:'sys_perc',
						self.IOWAIT_PERC:'iowait_perc',
						self.IDLE_PERC:'idle_perc',
						self.INTR_S:'intr_s',
						self.TIME_DATE:'time_date'}
		self.db_inst = {'host':('VARCHAR(255)', 'NULL'),
						'cpu':('INT', '0'),
						'user_perc':('DECIMAL(5,2)', '0.00'),
						'sys_perc':('DECIMAL(5,2)', '0.00'),
						'iowait_perc':('DECIMAL(5,2)', '0.00'),
						'idle_perc':('DECIMAL(5,2)', '0.00'),
						'intr_s':('INT', '0'),
						'time_date':('DECIMAL(13,6)', '0.000000')}
		self.db_descr = {'host':'system that is being monitored',
						'cpu':'number of cpu/processor being monitored',
						'user_perc':'percent of cpu being used by user',
						'sys_perc':'percent of cpu being used by system',
						'iowait_perc':'percent of cpu waiting',
						'idle_perc':'percent of cpu that is idle',
						'intr_s':'instructions (per second?)',
						'time_date':'time and date entry was updated'}
		self.var_flo = ('user_perc',
						'sys_perc',
						'iowait_perc',
						'idle_perc',
						'time_date')
		self.var_str = ('host',)
		self.var_int = ('cpu',
						'intr_s')
		self.table = 'cpu'
		self.values = '%s' + ',%s' * (len(self.db_list) - 1)

#dictionary of instantiated classes
instant_class = {'monitor_files':monitor_files(), 'ram':ram(), 'iostat':iostat(), 'cpu':cpu()}
all_classes = (monitor_files(), ram(), iostat(), cpu())

# Generate strings to load into query 
def fetch(info_list, db_dict, var_flo, var_str, var_int, table):
	# Instantiate variables to use to generate query string
	query = []
	searchstr = []

	# Info list should be ((field_name, search_field, option, (more_info)), 
	# Ex: (('cpu', NOSEARCH, EXACT, (2,)), ('user_perc', SEARCH, NONE, ()), ('intr_s', SEARCH, RANGE, (922, 935)))
	for item in info_list:
		if len(item) != 4:
			print 'ERROR -- LIST %s does not have enough entries' %(item)
			return None #HOW SHOULD I THROW ERRORS?

		# Instantiates field variable
		if isinstance(item[0], str):
			field = item[0]
		elif isinstance(item[0], int):
			field = db_dict[item[0]]

		if item[2] == EXACT:
			if len(item[3]) != 1:
				print 'ERROR -- LIST %s does not have the right amount of entries' %(item)
				return None #HOW SHOULD I THROW ERRORS?
	
			exact = item[3][0]

			# Adding info to lists to generate strings later
			if item[1] == SEARCH:
				query.append(field)
			if field in var_flo:
				searchstr.append('%s = %.6f'%(field, exact))
			elif field in var_str:
				searchstr.append("%s = '%s'"%(field, exact))
			elif field in var_int:
				searchstr.append('%s = %d'%(field, exact))

		elif item[2] == MIN:
			if len(item[3]) != 1:
				print 'ERROR -- LIST %s does not have the right amount of entries' %(item)
				return None #HOW SHOULD I THROW ERRORS?

			min = item[3][0]

			# Adding info to lists to generate strings later
			if item[1] == SEARCH:
				query.append(field)
			if field in var_flo:
				searchstr.append('%s >= %.6f'%(field, min))
			elif field in var_str:
				searchstr.append("%s >= '%s'"%(field, min))
			elif field in var_int:
				searchstr.append('%s >= %d'%(field, min))

		elif item[2] == MAX:
			if len(item[3]) != 1:
				print 'ERROR -- LIST %s does not have the right amount of entries' %(item) 
				return None #HOW SHOULD I THROW ERRORS?

			max = item[3][0]

			# Adding info to lists to generate strings later
			if item[1] == SEARCH:
				query.append(field)
			if field in var_flo:
				searchstr.append('%s <= %.6f'%(field, max))
			elif field in var_str:
				searchstr.append("%s <= '%s'"%(field, max))
			elif field in var_int:
				searchstr.append('%s <= %d'%(field, max))

		elif item[2] == RANGE:
			if len(item[3]) != 2:
				print 'ERROR -- LIST %s does not have the right amount of entries' %(item) 
				return None #HOW SHOULD I THROW ERRORS?

			min = item[3][0]
			max = item[3][1]

			# Adding info to lists to generate strings later
			if item[1] == SEARCH:
				query.append(field)
			if field in var_flo:
				searchstr.append('%s >= %.6f and %s <= %.6f'%(field, min, field, max))
			elif field in var_str:
				searchstr.append("%s >= '%s' and %s <= '%s'"%(field, min, field, max))
			elif field in var_int:
				searchstr.append('%s >= %d and %s <= %d'%(field, min, field, max))

		elif item[2] == LIST:
			if len(item[3]) <= 1:
				print 'ERROR -- LIST %s does not have the right amount of entries' %(item)
				return None #HOW SHOULD I THROW ERRORS?

			if item[1] == SEARCH:
				query.append(field)
			if field in var_flo:
				for it in item[3]:
					if it == item[3][0]:
						list_str = '(%s = %.6f' %(field, it)
					elif it == item[3][-1]:
						list_str = list_str + ' or %s = %.6f)' %(field, it)
					else:
						list_str = list_str + ' or %s = %.6f' %(field, it)
			elif field in var_str:
				for it in item[3]:
					if it == item[3][0]:
						list_str = "(%s = '%s'" %(field, it)
					elif it == item[3][-1]:
						list_str = list_str + " or %s = '%s')" %(field, it)
					else:
						list_str = list_str + " or %s = '%s'" %(field, it)
			elif field in var_int:
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

			# Adding info to lists to generate strings later
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
				dbstr = 'SELECT ' + item + ' FROM ' + table
			else:
				dbstr = 'SELECT ' + item + ' FROM ' + table + ' WHERE ' + qsearch
		else:
			if item == query[0]:
				dbstr = 'SELECT ' + item + ', '
			elif item == query[-1]:
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
