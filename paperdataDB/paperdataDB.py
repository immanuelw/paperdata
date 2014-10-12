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
# import paperdataDB as pdb
#files = pdb.dbsearch(pdb.fetch(list_of_info))
#OR combine dbsearch to include both
#files = pdf.dbseach(list_of_info)

def fields():
	field_list = ['path', 'era', 'era_type', 'obsnum', 'md5sum', 'julian_day', 'julian_date', 'polarization', 'data_length', 'raw_location', 'cal_location', 'tape_location', 'file_size_MB', 'raw_file_size_MB', 'compressed', 'ready_to_tape', 'delete_file', 'restore_history']

	return field_list

#def dict():
#	paperdata_dict = {}
#	return paperdata_dict

def options():
	opt = ['exact', 'min', 'max', 'range', 'none']
	return opt

#allows user to input database and table queried

usrnm = raw_input('Username: ')
pswd = getpass.getpass('Password: ')

def dbsearch(query):
	# open a database connection
	# be sure to change the host IP address, username, password and database name to match your own
	connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)

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

	#info list should be [[field_name, option, [more_info]], 
	#ex: [['era', 'exact', [32]], ['path', 'none', []], ['julian_day', 'range', [922, 935]]]
	for item in info_list:
		if len(item) != 3:
			print 'ERROR -- LIST %s does not have enough entries' %(item)
			sys.exit() #HOW SHOULD I THROW ERRORS?

		#instantiates field variable
		field = item[0]

		if item[1] == 'exact':
			if len(item[2]) != 1:
				print 'ERROR -- LIST %s does not have enough entries' %(item)
				sys.exit() #HOW SHOULD I THROW ERRORS?

			exact = item[2][0]

			#adding info to lists to generate strings later
			query.append(field)
			searchstr.append('%s = %d'%(field, exact))

		elif item[1] == 'min':
			if len(item[2]) != 1:
				print 'ERROR -- LIST %s does not have enough entries' %(item)
				sys.exit() #HOW SHOULD I THROW ERRORS?

			min = item[2][0]

			#adding info to lists to generate strings later
			query.append(field)
			searchstr.append('%s >= %d'%(field, min))

		elif item[1] == 'max':
			if len(item[2]) != 1:
				print 'ERROR -- LIST %s does not have enough entries' %(item) 
				sys.exit() #HOW SHOULD I THROW ERRORS?

			max = item[2][0]

			#adding info to lists to generate strings later
			query.append(field)
			searchstr.append('%s <= %d'%(field, max))

		elif item[1] == 'range':
			if len(item[2]) != 2:
				print 'ERROR -- LIST %s does not have enough entries' %(item) 
				sys.exit() #HOW SHOULD I THROW ERRORS?

			min = item[2][0]
			max = item[2][1]

			#adding info to lists to generate strings later
			query.append(field)
			searchstr.append('%s >= %d and %s <= %d'%(field, min, field, max))

		elif item[1] == 'none':
			if len(item[2]) != 1:
				print 'ERROR -- LIST %s does not have enough entries' %(item) 
				sys.exit() #HOW SHOULD I THROW ERRORS?

			#adding info to lists to generate strings later
			query.append(field)
			searchstr.append('')

		else:
			sys.exit() #HOW SHOULD I THROW ERRORS?

	for item in searchstr:
		if item == searchstr[0]:
			qsearch = item
		else:
			qsearch = qsearch + ' and ' + item

	for item in query:
		if item == query[0]:
			dbstr = 'SELECT ' + item + ','
		elif item == query[-1]:
			dbstr = dbstr + item + 'FROM paperdata WHERE' + qsearch

	return dbstr

#Only do things if running this script, not importing
#if __name__ == '__main__':
#
