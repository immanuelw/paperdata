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
class ram(self):
class iostat(self):
class cpu(self):
	self.db_list = []
	self.db_dict = {}
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
