#!/usr/bin/python
# -*- coding: utf-8 -*-

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass
import os
import csv

def consolidate(dbo)
	resultFile = open(dbo,'wb')
	resultFile.close()

	connection = MySQLdb.connect (host = 'shredder', user = 'paperboy', passwd = 'paperboy', db = 'paperdata', local_infile=True)
	cursor = connection.cursor()

	back = []
	front = []

	#path is [0]
	#jday is [5]
	#jdate is [6]
	#raw_location is [9]

	cursor.execute('SELECT * from paperdata where compressed = 1 order by julian_date asc')
	resa = cursor.fetchall()

	cursor.execute('SELECT * from paperdata where compressed = 0 order by julian_date asc')
	resb = cursor.fetchall()

	for item in resa:
		if item[9] != 'NULL':
			back.append(item)
		else:
			#do stuff
			cursor.execute('''SELECT * from paperdata where julian_date = '%.5f' and polarization = '%s' order by julian_date asc''' %(item[6], item[7]))
			ras = cursor.fetchall()
			for ra in ras:
				if len(ras) > 1:
					if item == ra:
						continue
				if len(ra) > 0:
					if ra[8] == 0.00000:
						length = item[8]
					else:
						length = ra[8]
					if item[10] == 'NULL':
						cal = ra[10]
					else:
						cal = item[10]
					it = (item[0],item[1],item[2],item[3],ra[4],item[5],item[6],ra[7],length,ra[9],cal,ra[11],item[12],ra[13],item[14],item[15],item[16],item[17],item[18])
					back.append(it)
				else:
					back.append(item)
	for ra in resb:
		if ra[0] != 'NULL':
			continue
		else:
			#do stuff
			cursor.execute('''SELECT * from paperdata where julian_date = '%.5f' and polarization = '%s' order by julian_date asc''' %(ra[6], ra[7]))
        	        items = cursor.fetchall()
			for item in items:
				if len(items) > 1:
					if item == ra:
						continue
	                	if len(item) > 0:
					if item[8] == 0.00000:
						length = ra[8]
					else:
						length = item[8]
					if ra[10] == 'NULL':
						cal = item[10]
					else:
						cal = ra[10]
	                        	it = (item[0],item[1],item[2],item[3],ra[4],item[5],item[6],ra[7],length,ra[9],cal,ra[11],item[12],ra[13],item[14],item[15],item[16],item[17],item[18])
	                        	front.append(it)
				else:
					front.append(ra)

	#Takes only unique entries
	total = tuple(back + front)
	backup = set(total)
	backup = sorted(backup, key=lambda x: (x[6], x[9], x[0]))

	for item in backup:
		resultFile = open(dbo,'ab')
		wr = csv.writer(resultFile, delimiter='|', dialect='excel')
		wr.writerow(item)
		resultFile.close()

	# Close database
	cursor.close()
	connection.close()

	return None

if __name__ == '__main__':
	dbo = '/data2/home/immwa/scripts/paperdata/single_entry.csv'
	consolidate(dbo)
