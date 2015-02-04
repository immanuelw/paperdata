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

# Function to create and output dictionary of results from query
def dbsearch_dict(query):
	# open a database connection
	# be sure to change the host IP address, username, password and database name to match your own
	connection = MySQLdb.connect (host = 'shredder', user = 'paperboy', passwd = 'paperboy', db = 'ganglia', local_infile=True)

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

# Function to create and output list of results from query
def dbsearch(query):
	# open a database connection
	# be sure to change the host IP address, username, password and database name to match your own
	connection = MySQLdb.connect (host = 'shredder', user = 'paperboy', passwd = 'paperboy', db = 'ganglia', local_infile=True)

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

class monitor_files(self):
	self.FILENAME = 11
	self.STATUS = 12
	self.DEL_TIME = 13
	self.STILL_HOST = 14
	self.TIME_DATE = 15
	self.db_list = ['filename', 'status', 'del_time', 'still_host', 'time_date']
	self.db_dict = {self.FILENAME:'filename', self.STATUS:'status', self.DEL_TIME'del_time', self.STILL_HOST'still_host', self.TIME_DATE:'time_date'}
	# Generate strings to load into query 
	def fetch(info_list):
		# Instantiate variables to use to generate query string
		query = []
		searchstr = []

		# Info list should be [[field_name, search_field, option, [more_info]], 
		# Ex: [['cpu', NOSEARCH, EXACT, [2]], ['user_perc', SEARCH, NONE, []], ['intr_s', SEARCH, RANGE, [922, 935]]]
		for item in info_list:
			if len(item) != 4:
				print 'ERROR -- LIST %s does not have enough entries' %(item)
				return None #HOW SHOULD I THROW ERRORS?

			# Instantiates field variable
			if isinstance(item[0], str):
				field = item[0]
			elif isinstance(item[0], int):
				field = self.db_dict[item[0]]

			if item[2] == EXACT:
				if len(item[3]) != 1:
					print 'ERROR -- LIST %s does not have the right amount of entries' %(item)
					return None #HOW SHOULD I THROW ERRORS?
	
				exact = item[3][0]

				# Adding info to lists to generate strings later
				if item[1] == SEARCH:
					query.append(field)
				if field in []:
					searchstr.append('%s = %.2f'%(field, exact))
				elif field in ['filename', 'status', 'still_host', 'time_date']:
					searchstr.append("%s = '%s'"%(field, exact))
				elif field in ['del_time']:
					searchstr.append('%s = %d'%(field, exact))

			elif item[2] == MIN:
				if len(item[3]) != 1:
					print 'ERROR -- LIST %s does not have the right amount of entries' %(item)
					return None #HOW SHOULD I THROW ERRORS?

				min = item[3][0]

				# Adding info to lists to generate strings later
				if item[1] == SEARCH:
					query.append(field)
				if field in []:
					searchstr.append('%s >= %.2f'%(field, min))
				elif field in ['filename', 'status', 'still_host', 'time_date']:
					searchstr.append("%s >= '%s'"%(field, min))
				elif field in ['del_time']:
					searchstr.append('%s >= %d'%(field, min))

			elif item[2] == MAX:
				if len(item[3]) != 1:
					print 'ERROR -- LIST %s does not have the right amount of entries' %(item) 
					return None #HOW SHOULD I THROW ERRORS?

				max = item[3][0]

				# Adding info to lists to generate strings later
				if item[1] == SEARCH:
					query.append(field)
				if field in []:
					searchstr.append('%s >= %.2f'%(field, max))
				elif field in ['filename', 'status', 'still_host', 'time_date']:
					searchstr.append("%s >= '%s'"%(field, max))
				elif field in ['del_time']:
					searchstr.append('%s >= %d'%(field, max))

			elif item[2] == RANGE:
				if len(item[3]) != 2:
					print 'ERROR -- LIST %s does not have the right amount of entries' %(item) 
					return None #HOW SHOULD I THROW ERRORS?

				min = item[3][0]
				max = item[3][1]

				# Adding info to lists to generate strings later
				if item[1] == SEARCH:

				if field in []:
					searchstr.append('%s >= %.2f and %s <= %.2f'%(field, min, field, max))
				elif field in ['filename', 'status', 'still_host', 'time_date']:
					searchstr.append("%s >= '%s' and %s <= '%s'"%(field, min, field, max))
				elif field in ['del_time']:
					searchstr.append('%s >= %d and %s <= %d'%(field, min, field, max))

			elif item[2] == LIST:
				if len(item[3]) <= 1:
					print 'ERROR -- LIST %s does not have the right amount of entries' %(item)
					return None #HOW SHOULD I THROW ERRORS?

				if item[1] == SEARCH:
					query.append(field)
				if field in []:
					for it in item[3]:
						if it == item[3][0]:
							list_str = '(%s = %.2f' %(field, it)
						elif it == item[3][-1]:
							list_str = list_str + ' or %s = %.2f)' %(field, it)
						else:
							list_str = list_str + ' or %s = %.2f' %(field, it)
				elif field in ['filename', 'status', 'still_host', 'time_date']:
					for it in item[3]:
						if it == item[3][0]:
							list_str = "(%s = '%s'" %(field, it)
						elif it == item[3][-1]:
							list_str = list_str + " or %s = '%s')" %(field, it)
						else:
							list_str = list_str + " or %s = '%s'" %(field, it)
				elif field in ['del_time']:
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
					dbstr = 'SELECT ' + item + ' FROM monitor_files'
				else:
					dbstr = 'SELECT ' + item + ' FROM monitor_files WHERE ' + qsearch
			else:
				if item == query[0]:
					dbstr = 'SELECT ' + item + ', '
				elif item == query[-1]:
					dbstr = dbstr + item + ' FROM monitor_files WHERE ' + qsearch
				else:
					dbstr = dbstr + item + ', '

		return dbstr

	# Near redundant class instantiation in order to facilitate easy repeatable access to paperdata through scripts
	# It's a way to code in queries without using the GUI whenever a change is wanted
	# REQUIRES knowledge of fields and paperdataDB constants

	class searchobj:
		def __init__(self):
			self.info_list = []
			return None
		def add_to_output(self, field, search_bool, limit, *range):
			self.info_list.append([field, search_bool, limit, range])
			return None
		def output(self):
			return dbsearch(fetch(self.info_list))
		def output_dict(self):
			return dbsearch_dict(fetch(self.info_list))

class ram(self):
	self.CPU = 11
	self.TOTAL = 12
	self.USED = 13
	self.FREE = 14
	self.SHARED = 15
	self.BUFFERS = 16
	self.CACHED = 17
	self.BC_USED = 18
	self.BC_FREE = 19
	self.SWAP_TOTAL = 20
	self.SWAP_USED = 21
	self.SWAP_FREE = 22
	self.TIME_DATE = 23
	self.db_list = ['cpu', 'total', 'used', 'free', 'shared', 'buffers', 'cached', 'bc_used', 'bc_free', 'swap_total', 'swap_used', 'swap_free', 'time_date']
	self.db_dict = {self.CPU:'cpu', self.TOTAL:'total', self.USED:'used', self.FREE:'free', self.SHARED:'shared', self.BUFFERS:'buffers', self.CACHED:'cached', self.BC_USED:'bc_used', self.BC_FREE:'bc_free', self.SWAP_TOTAL:'swap_total', self.SWAP_TOTAL:'swap_used', self.SWAP_FREE:'swap_free', self.TIME_DATE:'time_date'}
	# Generate strings to load into query 
	def fetch(info_list):
		# Instantiate variables to use to generate query string
		query = []
		searchstr = []

		# Info list should be [[field_name, search_field, option, [more_info]], 
		# Ex: [['cpu', NOSEARCH, EXACT, [2]], ['user_perc', SEARCH, NONE, []], ['intr_s', SEARCH, RANGE, [922, 935]]]
		for item in info_list:
			if len(item) != 4:
				print 'ERROR -- LIST %s does not have enough entries' %(item)
				return None #HOW SHOULD I THROW ERRORS?

			# Instantiates field variable
			if isinstance(item[0], str):
				field = item[0]
			elif isinstance(item[0], int):
				field = self.db_dict[item[0]]

			if item[2] == EXACT:
				if len(item[3]) != 1:
					print 'ERROR -- LIST %s does not have the right amount of entries' %(item)
					return None #HOW SHOULD I THROW ERRORS?
	
				exact = item[3][0]

				# Adding info to lists to generate strings later
				if item[1] == SEARCH:
					query.append(field)
				if field in []:
					searchstr.append('%s = %.2f'%(field, exact))
				elif field in ['time_date']:
					searchstr.append("%s = '%s'"%(field, exact))
				elif field in ['cpu', 'total', 'used', 'free', 'shared', 'buffers', 'cached', 'bc_used', 'bc_free', 'swap_total', 'swap_used', 'swap_free']:
					searchstr.append('%s = %d'%(field, exact))

			elif item[2] == MIN:
				if len(item[3]) != 1:
					print 'ERROR -- LIST %s does not have the right amount of entries' %(item)
					return None #HOW SHOULD I THROW ERRORS?

				min = item[3][0]

				# Adding info to lists to generate strings later
				if item[1] == SEARCH:
					query.append(field)
				if field in []:
					searchstr.append('%s >= %.2f'%(field, min))
				elif field in ['time_date']:
					searchstr.append("%s >= '%s'"%(field, min))
				elif field in ['cpu', 'total', 'used', 'free', 'shared', 'buffers', 'cached', 'bc_used', 'bc_free', 'swap_total', 'swap_used', 'swap_free']:
					searchstr.append('%s >= %d'%(field, min))

			elif item[2] == MAX:
				if len(item[3]) != 1:
					print 'ERROR -- LIST %s does not have the right amount of entries' %(item) 
					return None #HOW SHOULD I THROW ERRORS?

				max = item[3][0]

				# Adding info to lists to generate strings later
				if item[1] == SEARCH:
					query.append(field)
				if field in []:
					searchstr.append('%s <= %.2f'%(field, max))
				elif field in ['time_date']:
					searchstr.append("%s <= '%s'"%(field, max))
				elif field in ['cpu', 'total', 'used', 'free', 'shared', 'buffers', 'cached', 'bc_used', 'bc_free', 'swap_total', 'swap_used', 'swap_free']:
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
				if field in []:
					searchstr.append('%s >= %.2f and %s <= %.2f'%(field, min, field, max))
				elif field in ['time_date']:
					searchstr.append("%s >= '%s' and %s <= '%s'"%(field, min, field, max))
				elif field in ['cpu', 'total', 'used', 'free', 'shared', 'buffers', 'cached', 'bc_used', 'bc_free', 'swap_total', 'swap_used', 'swap_free']:
					searchstr.append('%s >= %d and %s <= %d'%(field, min, field, max))

			elif item[2] == LIST:
				if len(item[3]) <= 1:
					print 'ERROR -- LIST %s does not have the right amount of entries' %(item)
					return None #HOW SHOULD I THROW ERRORS?

				if item[1] == SEARCH:
					query.append(field)
				if field in []:
					for it in item[3]:
						if it == item[3][0]:
							list_str = '(%s = %.2f' %(field, it)
						elif it == item[3][-1]:
							list_str = list_str + ' or %s = %.2f)' %(field, it)
						else:
							list_str = list_str + ' or %s = %.2f' %(field, it)
				elif field in ['time_date']:
					for it in item[3]:
						if it == item[3][0]:
							list_str = "(%s = '%s'" %(field, it)
						elif it == item[3][-1]:
							list_str = list_str + " or %s = '%s')" %(field, it)
						else:
							list_str = list_str + " or %s = '%s'" %(field, it)
				elif field in ['cpu', 'total', 'used', 'free', 'shared', 'buffers', 'cached', 'bc_used', 'bc_free', 'swap_total', 'swap_used', 'swap_free']:
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
					dbstr = 'SELECT ' + item + ' FROM ram'
				else:
					dbstr = 'SELECT ' + item + ' FROM ram WHERE ' + qsearch
			else:
				if item == query[0]:
					dbstr = 'SELECT ' + item + ', '
				elif item == query[-1]:
					dbstr = dbstr + item + ' FROM ram WHERE ' + qsearch
				else:
					dbstr = dbstr + item + ', '

		return dbstr

	# Near redundant class instantiation in order to facilitate easy repeatable access to paperdata through scripts
	# It's a way to code in queries without using the GUI whenever a change is wanted
	# REQUIRES knowledge of fields and paperdataDB constants

	class searchobj:
		def __init__(self):
			self.info_list = []
			return None
		def add_to_output(self, field, search_bool, limit, *range):
			self.info_list.append([field, search_bool, limit, range])
			return None
		def output(self):
			return dbsearch(fetch(self.info_list))
		def output_dict(self):
			return dbsearch_dict(fetch(self.info_list))

class iostat(self):
	self.CPU = 11
	self.DEVICE = 12
	self.READ_S = 13
	self.WRITE_S = 14
	self.READS = 15
	self.WRITES = 16
	self.TIME_DATE = 17
	self.db_list = ['cpu', 'device', 'read_s', 'write_s', 'reads', 'writes', 'time_date']
	self.db_dict = {self.CPU:'cpu', self.DEVICE:'device', self.READ_S:'read_s', self.WRITE_S:'write_s', self.READS:'reads', self.WRTIES:'writes', self.TIME_DATE:'time_date'}
	# Generate strings to load into query 
	def fetch(info_list):
		# Instantiate variables to use to generate query string
		query = []
		searchstr = []

		# Info list should be [[field_name, search_field, option, [more_info]], 
		# Ex: [['cpu', NOSEARCH, EXACT, [2]], ['user_perc', SEARCH, NONE, []], ['intr_s', SEARCH, RANGE, [922, 935]]]
		for item in info_list:
			if len(item) != 4:
				print 'ERROR -- LIST %s does not have enough entries' %(item)
				return None #HOW SHOULD I THROW ERRORS?

			# Instantiates field variable
			if isinstance(item[0], str):
				field = item[0]
			elif isinstance(item[0], int):
				field = self.db_dict[item[0]]

			if item[2] == EXACT:
				if len(item[3]) != 1:
					print 'ERROR -- LIST %s does not have the right amount of entries' %(item)
					return None #HOW SHOULD I THROW ERRORS?
	
				exact = item[3][0]

				# Adding info to lists to generate strings later
				if item[1] == SEARCH:
					query.append(field)
				if field in ['user_perc', 'sys_perc', 'iowait_perc', 'idle_perc']:
					searchstr.append('%s = %.2f'%(field, exact))
				elif field in ['time_date']:
					searchstr.append("%s = '%s'"%(field, exact))
				elif field in ['cpu', 'intr_s']:
					searchstr.append('%s = %d'%(field, exact))

			elif item[2] == MIN:
				if len(item[3]) != 1:
					print 'ERROR -- LIST %s does not have the right amount of entries' %(item)
					return None #HOW SHOULD I THROW ERRORS?

				min = item[3][0]

				# Adding info to lists to generate strings later
				if item[1] == SEARCH:
					query.append(field)
				if field in ['user_perc', 'sys_perc', 'iowait_perc', 'idle_perc']:
					searchstr.append('%s >= %.2f'%(field, min))
				elif field in ['time_date']:
					searchstr.append("%s >= '%s'"%(field, min))
				elif field in ['cpu', 'intr_s']:
					searchstr.append('%s >= %d'%(field, min))

			elif item[2] == MAX:
				if len(item[3]) != 1:
					print 'ERROR -- LIST %s does not have the right amount of entries' %(item) 
					return None #HOW SHOULD I THROW ERRORS?

				max = item[3][0]

				# Adding info to lists to generate strings later
				if item[1] == SEARCH:
					query.append(field)
				if field in ['user_perc', 'sys_perc', 'iowait_perc', 'idle_perc']:
					searchstr.append('%s <= %.2f'%(field, max))
				elif field in ['time_date']:
					searchstr.append("%s <= '%s'"%(field, max))
				elif field in ['cpu', 'intr_s']:
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
				if field in ['user_perc', 'sys_perc', 'iowait_perc', 'idle_perc']:
					searchstr.append('%s >= %.2f and %s <= %.2f'%(field, min, field, max))
				elif field in ['time_date']:
					searchstr.append("%s >= '%s' and %s <= '%s'"%(field, min, field, max))
				elif field in ['cpu', 'intr_s']:
					searchstr.append('%s >= %d and %s <= %d'%(field, min, field, max))

			elif item[2] == LIST:
				if len(item[3]) <= 1:
					print 'ERROR -- LIST %s does not have the right amount of entries' %(item)
					return None #HOW SHOULD I THROW ERRORS?

				if item[1] == SEARCH:
					query.append(field)
				if field in ['user_perc', 'sys_perc', 'iowait_perc', 'idle_perc']:
					for it in item[3]:
						if it == item[3][0]:
							list_str = '(%s = %.2f' %(field, it)
						elif it == item[3][-1]:
							list_str = list_str + ' or %s = %.2f)' %(field, it)
						else:
							list_str = list_str + ' or %s = %.2f' %(field, it)
				elif field in ['time_date']:
					for it in item[3]:
						if it == item[3][0]:
							list_str = "(%s = '%s'" %(field, it)
						elif it == item[3][-1]:
							list_str = list_str + " or %s = '%s')" %(field, it)
						else:
							list_str = list_str + " or %s = '%s'" %(field, it)
				elif field in ['cpu', 'intr_s']:
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
					dbstr = 'SELECT ' + item + ' FROM iostat'
				else:
					dbstr = 'SELECT ' + item + ' FROM iostat WHERE ' + qsearch
			else:
				if item == query[0]:
					dbstr = 'SELECT ' + item + ', '
				elif item == query[-1]:
					dbstr = dbstr + item + ' FROM iostat WHERE ' + qsearch
				else:
					dbstr = dbstr + item + ', '

		return dbstr

	# Near redundant class instantiation in order to facilitate easy repeatable access to paperdata through scripts
	# It's a way to code in queries without using the GUI whenever a change is wanted
	# REQUIRES knowledge of fields and paperdataDB constants

	class searchobj:
		def __init__(self):
			self.info_list = []
			return None
		def add_to_output(self, field, search_bool, limit, *range):
			self.info_list.append([field, search_bool, limit, range])
			return None
		def output(self):
			return dbsearch(fetch(self.info_list))
		def output_dict(self):
			return dbsearch_dict(fetch(self.info_list))

class cpu(self):
	self.CPU = 11
	self.USER_PERC = 12
	self.SYS_PERC = 13
	self.IOWAIT_PERC = 14
	self.IDLE_PERC = 15
	self.INTR_S = 16
	self.TIME_DATE = 17
	self.db_list = ['cpu', 'user_perc', 'sys_perc', 'iowait_perc', 'idle_perc', 'intr_s', 'time_date']
	self.db_dict = {self.CPU:'cpu', self.USER_PERC:'user_perc', self.SYS_PERC:'sys_perc', self.IOWAIT_PERC:'iowait_perc', self.IDLE_PERC:'idle_perc', self.INTR_S:'intr_s', self.TIME_DATE:'time_date'}
	# Generate strings to load into query 
	def fetch(info_list):
		# Instantiate variables to use to generate query string
		query = []
		searchstr = []

		# Info list should be [[field_name, search_field, option, [more_info]], 
		# Ex: [['cpu', NOSEARCH, EXACT, [2]], ['user_perc', SEARCH, NONE, []], ['intr_s', SEARCH, RANGE, [922, 935]]]
		for item in info_list:
			if len(item) != 4:
				print 'ERROR -- LIST %s does not have enough entries' %(item)
				return None #HOW SHOULD I THROW ERRORS?

			# Instantiates field variable
			if isinstance(item[0], str):
				field = item[0]
			elif isinstance(item[0], int):
				field = self.db_dict[item[0]]

			if item[2] == EXACT:
				if len(item[3]) != 1:
					print 'ERROR -- LIST %s does not have the right amount of entries' %(item)
					return None #HOW SHOULD I THROW ERRORS?
	
				exact = item[3][0]

				# Adding info to lists to generate strings later
				if item[1] == SEARCH:
					query.append(field)
				if field in ['user_perc', 'sys_perc', 'iowait_perc', 'idle_perc']:
					searchstr.append('%s = %.2f'%(field, exact))
				elif field in ['time_date']:
					searchstr.append("%s = '%s'"%(field, exact))
				elif field in ['cpu', 'intr_s']:
					searchstr.append('%s = %d'%(field, exact))

			elif item[2] == MIN:
				if len(item[3]) != 1:
					print 'ERROR -- LIST %s does not have the right amount of entries' %(item)
					return None #HOW SHOULD I THROW ERRORS?

				min = item[3][0]

				# Adding info to lists to generate strings later
				if item[1] == SEARCH:
					query.append(field)
				if field in ['user_perc', 'sys_perc', 'iowait_perc', 'idle_perc']:
					searchstr.append('%s >= %.2f'%(field, min))
				elif field in ['time_date']:
					searchstr.append("%s >= '%s'"%(field, min))
				elif field in ['cpu', 'intr_s']:
					searchstr.append('%s >= %d'%(field, min))

			elif item[2] == MAX:
				if len(item[3]) != 1:
					print 'ERROR -- LIST %s does not have the right amount of entries' %(item) 
					return None #HOW SHOULD I THROW ERRORS?

				max = item[3][0]

				# Adding info to lists to generate strings later
				if item[1] == SEARCH:
					query.append(field)
				if field in ['user_perc', 'sys_perc', 'iowait_perc', 'idle_perc']:
					searchstr.append('%s <= %.2f'%(field, max))
				elif field in ['time_date']:
					searchstr.append("%s <= '%s'"%(field, max))
				elif field in ['cpu', 'intr_s']:
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
				if field in ['user_perc', 'sys_perc', 'iowait_perc', 'idle_perc']:
					searchstr.append('%s >= %.2f and %s <= %.2f'%(field, min, field, max))
				elif field in ['time_date']:
					searchstr.append("%s >= '%s' and %s <= '%s'"%(field, min, field, max))
				elif field in ['cpu', 'intr_s']:
					searchstr.append('%s >= %d and %s <= %d'%(field, min, field, max))

			elif item[2] == LIST:
				if len(item[3]) <= 1:
					print 'ERROR -- LIST %s does not have the right amount of entries' %(item)
					return None #HOW SHOULD I THROW ERRORS?

				if item[1] == SEARCH:
					query.append(field)
				if field in ['user_perc', 'sys_perc', 'iowait_perc', 'idle_perc']:
					for it in item[3]:
						if it == item[3][0]:
							list_str = '(%s = %.2f' %(field, it)
						elif it == item[3][-1]:
							list_str = list_str + ' or %s = %.2f)' %(field, it)
						else:
							list_str = list_str + ' or %s = %.2f' %(field, it)
				elif field in ['time_date']:
					for it in item[3]:
						if it == item[3][0]:
							list_str = "(%s = '%s'" %(field, it)
						elif it == item[3][-1]:
							list_str = list_str + " or %s = '%s')" %(field, it)
						else:
							list_str = list_str + " or %s = '%s'" %(field, it)
				elif field in ['cpu', 'intr_s']:
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
					dbstr = 'SELECT ' + item + ' FROM cpu'
				else:
					dbstr = 'SELECT ' + item + ' FROM cpu WHERE ' + qsearch
			else:
				if item == query[0]:
					dbstr = 'SELECT ' + item + ', '
				elif item == query[-1]:
					dbstr = dbstr + item + ' FROM cpu WHERE ' + qsearch
				else:
					dbstr = dbstr + item + ', '

		return dbstr

	# Near redundant class instantiation in order to facilitate easy repeatable access to paperdata through scripts
	# It's a way to code in queries without using the GUI whenever a change is wanted
	# REQUIRES knowledge of fields and paperdataDB constants

	class searchobj:
		def __init__(self):
			self.info_list = []
			return None
		def add_to_output(self, field, search_bool, limit, *range):
			self.info_list.append([field, search_bool, limit, range])
			return None
		def output(self):
			return dbsearch(fetch(self.info_list))
		def output_dict(self):
			return dbsearch_dict(fetch(self.info_list))

#Only do things if running this script, not importing
if __name__ == '__main__':
	print 'Not a script file, just a module'
