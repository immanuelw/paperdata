#!/usr/bin/python
# -*- coding: utf-8 -*-
# Update data in MySQL table 

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass

datab = raw_input('Database:')
table = raw_input('Update data in table named:')
pswd = getpass.getpass('Password:')

compressed = 'compressed'
obsnum_string = 'obsnum'
tape = 'ready_to_tape'

count_jday = 0
count_complete = 0

jday_results = []

#checks if files of the same Julian Date have all completed compression
def complete_check(count_jday, count_complete, jday_results):
        if count_jday == count_complete and not count_jday == 0:
                ready_to_tape = True
                for items in jday_results:
                        obsnum = items[0]
                        # execute the SQL query using execute() method.
                        cursor.execute('''
                        UPDATE %s
                        SET %s = %s
                        WHERE %s = %d;
                        '''%(table, tape, ready_to_tape, obsnum_string, obsnum))

#need way to get compr_value and obsnum from paperdistiller 

# open a database connection
connection = MySQLdb.connect (host = 'shredder', passwd = pswd, db = 'paperdistiller', local_infile=True)

cursor = connection.cursor()

# execute the SQL query using execute() method.
cursor.execute('SELECT obsnum, status, julian_date from observations order by julian_date')

#collects information from query
results = cursor.fetchall()

#close, save and end connection
cursor.close()
connection.commit()
connection.close()

# open a database connection
# be sure to change the host IP address, username, password and database name to match your own
connection = MySQLdb.connect (host = 'shredder', passwd = pswd, db = datab, local_infile=True)

# prepare a cursor object using cursor() method
cursor = connection.cursor()

#results is a list of lists
for items in results:

	count_jday = len(jday_results)
	#sets value of initial julian day found
	if count_jday == 0:
		julian_day = int(str(items[2])[3:7]) #error if psa32
	
        obsnum = items[0]

	j_day = int(str(items[2])[3:7])

	#counts amount of files with same Julian Day
	if j_day == julian_day:
		jday_results.append(items)
		#checks if file is done compression
	        if items[1] == 'COMPLETE':
	                compr_value = True
			#counts amount of files complete
			count_complete += 1
	        else:
	                compr_value = False

		# execute the SQL query using execute() method.
		cursor.execute('''
		UPDATE %s
		SET %s = %s
		WHERE %s = %d;
		'''%(table, compressed, compr_value, obsnum_string, obsnum)) 
		###change so %d if number or %s if string entry!!!
	else:
		complete_check(count_jday, count_complete, jday_results)	
		jday_results = []
		jday_results.append(items)
		julian_day = j_day
		count_jday = 0
		count_complete = 0
		items[1] == 'COMPLETE':
                        compr_value = True
                        count_complete += 1
                else:
                        compr_value = False

                cursor.execute('''
                UPDATE %s
                SET %s = %s
                WHERE %s = %d;
                '''%(table, compressed, compr_value, obsnum_string, obsnum))

print 'Table data updated.'

# close the cursor object
cursor.close()

#save changes to database
connection.commit()

# close the connection
connection.close()

# exit the program
sys.exit()
