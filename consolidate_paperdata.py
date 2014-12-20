#!/usr/bin/python
# -*- coding: utf-8 -*-

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass
import os
import csv

def clean_paperdata(usrnm, pswd):
	connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)
	cursor = connection.cursor()

	cursor.execute('''SELECT path, raw_path, npz_path, final_product_path from paperdata where (path like 'folio%' or raw_path like 'folio%')''')
	results = cursor.fetchall()
	for item in results:
		path = item[0]
		raw_path = item[1]
		npz_path = item[2]
		final_product_path = item[3]
		if not os.path.isdir(path.split(':')[1]):
			cursor.execute('''UPDATE paperdata SET path = 'NULL' where path = '%s' ''' %(path))
		if not os.path.isdir(raw_path.split(':')[1]):
			cursor.execute('''UPDATE paperdata SET raw_path = 'NULL' where raw_path = '%s' ''' %(raw_path))
		if not os.path.isfile(npz_path.split(':')[1]):
			cursor.execute('''UPDATE paperdata SET npz_path = 'NULL' where npz_path = '%s' ''' %(npz_path))
		if not os.path.isdir(final_product_path.split(':')[1]):
			cursor.execute('''UPDATE paperdata SET final_product_path = 'NULL' where final_product_path = '%s' ''' %(final_product_path))

	cursor.execute('''DELETE FROM paperdata where raw_path = 'NULL' and path = 'NULL' ''')
	# Close and save database
	cursor.close()
	connection.commit()
	connection.close()

	return None

def generate_entry_1(item):
	if item[0] in ['NULL', 'ON TAPE']:
		path = item[0]
	else:
		compr_host = item[0].split(':')[0]
		compr = item[0].split(':')[1]
		if compr_host == 'folio' and not os.path.isdir(compr):
			path = 'NULL'
		else:
			path = item[0]
	if item[9] in  ['NULL', 'ON TAPE']:
		raw_path = item[9]
	else:
		raw_host = item[9].split(':')[0]
		raw = item[9].split(':')[1]
		if raw_host == 'folio' and not os.path.isdir(raw):
			raw_path = 'NULL'
		else:
			raw_path = item[9]

	if raw_path == 'NULL' and path == 'NULL':
		return None

	if raw_path == 'NULL':
		raw_sz = 0.0
	else:
		raw_sz = item[15]

	if path == 'NULL':
		compr_sz = 0.0
	else:
		compr_sz = item[14]

	try:
		tape = item[13].split('"')[1]
	except:
		tape = item[13]

	it = (path,item[1],item[2],item[3],item[4],item[5],item[6],item[7],item[8],raw_path,item[10],item[11],item[12],tape,compr_sz,raw_sz,item[16],item[17],item[18],item[19],item[20],item[21])

	return it

def generate_entry_2(item, ra):
	if ra[8] == 0.00000:
		length = item[8]
	else:
		length = ra[8]
	if item[10] == 'NULL':
		cal = ra[10]
	else:
		cal = item[10]

	if item[0] == 'NULL':
		path = 'NULL'
		obsnum = ra[3]
	else:
		compr_host = item[0].split(':')[0]
		compr = item[0].split(':')[1]
		if compr_host == 'folio' and not os.path.isdir(compr):
			path = 'NULL'
			obsnum = ra[3]
		else:
			path = item[0]
			obsnum = item[3]
	if ra[9] == 'NULL':
		raw_path = 'NULL'
	else:
		raw_host = ra[9].split(':')[0]
		raw = ra[9].split(':')[1]
		if raw_host == 'folio' and not os.path.isdir(raw):
			raw_path = 'NULL'
		else:
			raw_path = ra[9]

	if raw_path == 'NULL' and path == 'NULL':
		return None

	if raw_path == 'NULL':
		raw_sz = 0.0
	else:
		raw_sz = ra[15]

	if path == 'NULL':
		compr_sz = 0.0
	else:
		compr_sz = item[14]

	try:
		tape = ra[13].split('"')[1]
	except:
		tape = ra[13]

	it = (path,item[1],item[2],obsnum,ra[4],ra[5],ra[6],ra[7],length,raw_path,cal,item[11],item[12],tape,compr_sz,raw_sz,item[16],ra[17],ra[18],ra[19],ra[20],item[21])
	return it

def consolidate(dbo):
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
			it = generate_entry_1(item)
			if it is None:
				continue
			else:
				back.append(it)
		else:
			#do stuff
			zen = '%' + item[0].split('/')[-1][:-4] + '%'
			cursor.execute('''SELECT * from paperdata where raw_path like '%s' order by julian_date asc''' %(zen))
			ras = cursor.fetchall()
			for ra in ras:
				if len(ras) > 1:
					if item == ra:
						continue
				if len(ra) > 0:
					it = generate_entry_2(item,ra)
					if it is None:
						continue
					else:
						back.append(it)
			if len(ras) == 0:
				it = generate_entry_1(item)
				if it is None:
					continue
				else:
					front.append(it)
	for ra in resb:
		if ra[0] != 'NULL':
			continue
		else:
			#do stuff
			zen = '%' + ra[9].split('/')[-1] + '%'
			cursor.execute('''SELECT * from paperdata where path like '%s' order by julian_date asc''' %(zen))
			items = cursor.fetchall()
			for item in items:
				if len(items) > 1:
					if item == ra:
						continue
				if len(item) > 0:
					it = generate_entry_2(item,ra)
					if it is None:
						continue
					else:
						front.append(it)
			if len(items) == 0:
				it = generate_entry_1(ra)
				if it is None:
					continue
				else:
					front.append(it)
	#Takes only unique entries
	total = tuple(back + front)
	backup = set(total)
	backup = sorted(backup, key=lambda x: (x[6], x[9], x[0]))

	for item in backup:
		resultFile = open(dbo,'ab')
		wr = csv.writer(resultFile, delimiter='|', lineterminator='\n', dialect='excel')
		wr.writerow(item)
		resultFile.close()

	# Close database
	cursor.close()
	connection.close()

	return None

if __name__ == '__main__':
	dbo = '/data2/home/immwa/scripts/paperdata/single_entry.psv'
	clean = raw_input('Clean?(y/n): ')
	if clean == 'y':
		usrnm = raw_input('Username: ')
		pswd = getpass.getpass('Password: ')
		clean_paperdata(usrnm, pswd)
	consolidate(dbo)
