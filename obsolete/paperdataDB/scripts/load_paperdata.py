#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass
import os
import csv
import aipy as A
import hashlib
import glob
import socket
import decimal
import paperdataDB as pdb

### Script to load data from folio into paperdata database
### Crawls folio and reads through .uvcRRE or .uv files to generate all field information

### Author: Immanuel Washington
### Date: 8-20-14

#Functions which simply find the file size of the .uvcRRE files
def get_size(start_path):
	total_size = 0
	for dirpath, dirnames, filenames in os.walk(start_path):
		for f in filenames:
			fp = os.path.join(dirpath, f)
			total_size += os.path.getsize(fp)
	return total_size

def sizeof_fmt(num):
	for x in ['bytes','KB','MB']:
		if num < 1024.0:
			return "%3.1f" % (num)
		num /= 1024.0
	num *= 1024.0
	return "%3.1f" % (num)

#create function to uniquely identify files
def jdpol2obsnum(jd,pol,djd):
	"""
	input: julian date float, pol string. and length of obs in fraction of julian date
	output: a unique index
	"""
	dublinjd = jd - 2415020  #use Dublin Julian Date
	obsint = int(dublinjd/djd)  #divide up by length of obs
	polnum = A.miriad.str2pol[pol]+10
	assert(obsint < 2**31)
	return int(obsint + polnum*(2**32))

def md5sum(fname):
	"""
	calculate the md5 checksum of a file whose filename entry is fname.
	"""
	fname = fname.split(':')[-1]
	BLOCKSIZE = 65536
	hasher = hashlib.md5()
	try:
		afile = open(fname, 'rb')
	except(IOError):
		afile = open("%s/visdata"%fname, 'rb')
	buf = afile.read(BLOCKSIZE)
	while len(buf) >0:
		hasher.update(buf)
		buf = afile.read(BLOCKSIZE)
	return hasher.hexdigest()

def load_db_from_file(dbo, table, usrnm, pswd):
	#Load data into named database and table
	# open a database connection
	connection = MySQLdb.connect (host = 'shredder', user = usrnm, password = pswd, database = 'paperdata')

	# prepare a cursor object using cursor() method
	cursor = connection.cursor()

	outfile = dbo %(table)

	#execute the SQL query using execute() method.
	insert_base = '''INSERT INTO %s VALUES(%s)'''

	if table not in pdb.classes:
		print 'Incorrect table name'
		return None
	var_class = pdb.instant_class[table]
	table_vars = (var_class.table, var_class.values)

	#load information from file into eatwell database
	with open(outfile, 'rb') as read_file:
		read = csv.reader(read_file, delimiter='|', lineterminator='\n', dialect='excel')
		for row in read:
			val = tuple(row)
			insert = insert_base % table_vars
			values = val
			cursor.execute(insert, values)

	print 'Table data loaded.'

	#Close and save changes to database
	cursor.close()
	connection.commit()
	connection.close()

	return None

def obsnum_fixer(usrnm, pswd):
	pol_dict = {-5:'xx',-6:'yy',-7:'xy',-8:'yx'}
	connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)
	cursor = connection.cursor()
	cursor.execute('''SELECT julian_day, count(*) from paperdata group by julian_day order by julian_day asc''')
	results = cursor.fetchall()

	jday = [int(item[0]) for item in results if int(item[1]) > 60]

	for day in jday:
		cursor.execute('''SELECT julian_date, polarization from paperdata where julian_day = %s and raw_path like '%s' group by julian_date, polarization order by julian_date asc''', (day,'folio%'))

		jd_pols = cursor.fetchall()
		#collects information from query
		jd_one = [round(float(jd),5) for jd,pol in jd_pols if pol == jd_pols[0][1]]

		length = n.mean(n.diff(jd_one))
		print length
		cursor.execute('''SELECT raw_path, obsnum from paperdata where julian_day = %s and raw_path like '%s' group by julian_date, polarization order by julian_date asc''', (day,'folio%'))
		results = cursor.fetchall()

		#results is a list of lists
		for item in results:
			full_path = item[0]
			path = item[0].split(':')[1]

			#Random error
			if path in ['/mnt/WilliamHenryHarrison/psa6250/zen.2456250.16694.uv', '/nas1/data/psa6644/zen.2456644.18779.xy.uv', '/nas1/data/psa6644/zen.2456644.19475.xx.uv', '/nas1/data/psa6644/zen.2456644.19475.xx.uv', '/nas2/data/psa6650/zen.2456650.16694.xx.uv', '/nas2/data/psa6650/zen.2456650.16694.xy.uv']:
				continue

			#allows uv access
			try:
				uv = A.miriad.UV(path)
			except:
				continue
			try:
				jdate = uv['time']
			except:
				continue

			#assign letters to each polarization
			if uv['npol'] == 1:
				polarization = pol_dict[uv['pol']]
			elif uv['npol'] == 4:
				#	   polarization = 'all' #default to 'yy' as 'all' is not a key for jdpol2obsnum
				polarization = 'yy'
			#variable to input into jdpol2obsnum
			divided_jdate = length

			#gives each file unique id
			if length > 0:
				obsnum = jdpol2obsnum(jdate,polarization,divided_jdate)
			else:
				obsnum = 0
			cursor.execute('''UPDATE paperdata set obsnum = %d, data_length = %.5f where raw_path = %s ''', (obsnum, length, full_path))
	cursor.close()
	connection.commit()
	connection.close()

	return None

def gen_paperdata(dirs, dbo, dbe):
	host = socket.gethostname()

	#Erase former data file
	data_file = open(dbo,'wb')
	data_file.close()

	full_info = []
	#Dictionary of polarizations
	pol_dict = {-5:'xx',-6:'yy',-7:'xy',-8:'yx'}

	#List of files that have gone through
	completed_dirs = []

	for dir in dirs[:]:

		if dir in completed_dirs:
			continue

		#create csv file to log data
		data_file = open(dbo,'ab')
		wr = csv.writer(data_file, delimiter='|', lineterminator='\n', dialect='excel')

		#create csv file to log bad files
		error_file = open(dbe, 'ab')
		ewr = csv.writer(error_file, delimiter='|', lineterminator='\n', dialect='excel')

		#checks if file loaded in is raw or compressed - makes changes to compensate
		if dir.split('.')[-1] == 'uvcRRE':
			print dir
			#indicates name of full directory
			compr_full_path = host + ':' + dir
			compr_path = compr_full_path.split(':')[1]
			raw_full_path = compr_full_path[:-4]
			raw_path = dir[:-4]
			raw_file = os.path.join(raw_path, 'visdata')
			path = raw_path
			if not os.path.isfile(raw_file):
				raw_full_path = 'NULL'
				path = compr_path

		elif dir.split('.')[-1] == 'uv':
			print dir
			#indicates name of full directory -- SHOULD I SET TO NULL? OR CHECK DATABASE EVERY TIME?
			raw_full_path = host + ':' + dir
			raw_path = dir
			compr_full_path = host + ':' + dir + 'cRRE'
			compr_path = dir + 'cRRE'
			compr_file = os.path.join(compr_path, 'visdata')
			path = raw_path
			if not os.path.isfile(compr_file):
				compr_full_path = 'NULL'
		else:
			continue

		#checks to make sure file can be accessed

		#temp fix
		if dir == '/nas2/data/psa6668/zen.2456668.17386.yx.uvcRRE':
			item = [path,'Unknown error']
			ewr.writerow(item)
			error_file.close()
			continue

		#checks a .uv file for data
		visdata = os.path.join(path, 'visdata')
		if not os.path.isfile(visdata):
			item = [path,'No visdata']
			ewr.writerow(item)
			error_file.close()
			continue

		#checks a .uv file for vartable
		vartable = os.path.join(path, 'vartable')
		if not os.path.isfile(vartable):
			item = [path,'No vartable']
			ewr.writerow(item)
			error_file.close()
			continue

		#checks a .uv file for header
		header = os.path.join(path, 'header')
		if not os.path.isfile(header):
			item = [path,'No header']
			ewr.writerow(item)
			error_file.close()
			continue

		#allows uv access
		try:
			uv = A.miriad.UV(path)
		except:
			item = [path,'Cannot access .uv file']
			ewr.writerow(item)
			error_file.close()
			continue	

		#indicates size of compressed file, removing units
		if compr_full_path != 'NULL':
			lil_byte = sizeof_fmt(get_size(compr_path))
			compr_file_size = round(float(lil_byte), 1)

			compressed = 1

		else:
			compr_file_size = 0.0
			compressed = 0

		#indicates size of raw file, removing units
		if raw_full_path != 'NULL':
			lil_byte = sizeof_fmt(get_size(raw_path))
			raw_file_size = round(float(lil_byte), 1)

			#calculate md5sum
			mdsum = md5sum(raw_path)
		else:
			raw_file_size = 0.0
			mdsum = 'NULL'

		#indicates julian date
		jdate = round(uv['time'], 5)

		#indicates julian day and set of data
		if jdate < 2456100:
			era = 32
		elif jdate < 2456400:
			era = 64
		else:
			era = 128

		jday = int(str(jdate)[3:7])

		#indicates type of file in era
		era_type = 'NULL'

		#assign letters to each polarization
		if uv['npol'] == 1:
			polarization = pol_dict[uv['pol']]
		elif uv['npol'] == 4:
		#	polarization = 'all' #default to 'yy' as 'all' is not a key for jdpol2obsnum
			polarization = 'yy'

		t_min = 0
		t_max = 0
		n_times = 0
		c_time = 0

		try:
			for (uvw, t, (i,j)),d in uv.all():
				if t_min == 0 or t < t_min:
					t_min = t
				if t_max == 0 or t > t_max:
					t_max = t
				if c_time != t:
					c_time = t
					n_times += 1
		except:
			item = [path, 'Cannot read through .uv file']
			ewr.writerow(item)
			error_file.close()
			continue

		if n_times > 1:
			dt = -(t_min - t_max)/(n_times - 1)
		else:
			dt = -(t_min - t_max)/(n_times)

		length = n_times * dt
		#round so fits obsnum
		length = round(length, 19)

		#variable to input into jdpol2obsnum
		divided_jdate = length

		#gives each file unique id
		if length > 0:
			obsnum = jdpol2obsnum(jdate,polarization,divided_jdate)
		else:
			obsnum = 0

		#location of calibrate files
		if era == 32:
			cal_path = '/usr/global/paper/capo/arp/calfiles/psa898_v003.py'
		elif era == 64:
			cal_path = '/usr/global/paper/capo/zsa/calfiles/psa6240_v003.py'
		elif era == 128:
			cal_path = 'NULL'

		if compr_full_path != 'NULL':
			#shows path of npz file
			npz_path = compr_full_path.split('uvcRRE')[0] + 'uvcRE.npz'
			if not os.path.isfile(npz_path.split(':')[1]):
				npz_path = 'NULL'

			#shows path of final product
			if era == 32:
				final_product_path = compr_full_path.split('uvcRRE')[0] + 'uvcRREzCPSBx'
				if not os.path.isdir(final_product_path.split(':')[1]):
					final_product_path = 'NULL'
			else:
				 final_product_path = 'NULL'

		else:
			npz_path = 'NULL'
			final_product_path = 'NULL'

		#shows index of raw data on tape
		tape_index = 'NULL'

		#Shows if file is edge file
		edge = 0

		#variable indicating if all files have been successfully compressed in one day
		write_to_tape = 0

		#indicates if all raw data is compressed, moved to tape, and the raw data can be deleted from folio
		delete_file = 0 

		#indicates times the file has been restored
		restore_history = 'NULL'

		#comment about file
		comment = ''

		#create list of important data and open csv file
		databs = (compr_full_path,
					era,
					era_type,
					obsnum,
					mdsum,
					jday,
					jdate,
					polarization,
					length,
					raw_full_path,
					cal_path,
					npz_path,
					final_product_path,
					tape_index,
					compr_file_size,
					raw_file_size,
					compressed,
					edge,
					write_to_tape,
					delete_file,
					restore_history,
					comment)
		print databs

		#write to csv file
		wr.writerow(databs)
		full_info.append(databs)

		#Remove corresponding file from list
		if dir.split('.')[-1] == 'uvcRRE':
			try:
				completed_dirs.append(dir[:4])
			except:
				data_file.close()
				continue		

		if dir.split('.')[-1] == 'uv':
			try:
				completed_dirs.append(dir + 'cRRE')
			except:
				data_file.close()
				continue
		#save into file and close it
		data_file.close()


	return full_info

def remove_duplicates(dirs_all, usrnm, pswd):
	#Removes all files from list that already exist in the database
	connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)

	cursor = connection.cursor()

	cursor.execute('''SELECT path, raw_path from paperdata''')
	results = cursor.fetchall()
	cursor.close()
	connection.close()

	for res in results:
		if res[0] != 'NULL':
			folderC = res[0].split(':')[1]
		else:
			folderC = 'NULL'
		if res[1] != 'NULL':
			folderR = res[1].split(':')[1]
		else:
			folderR = 'NULL'

		try:
			dirs_all.remove(folderR)
		except:
			try:
				dirs_all.remove(folderC)
			except:
				continue

		try:
			dirs_all.remove(folderC)
		except:
			continue

	return dirs_all

def load_paperdata(auto):
	#User input information
	usrnm = 'paperboy'
	pswd = 'paperboy'

	datanum = raw_input('Input file path: ')

	dbo = '/data4/paper/paperoutput/db_out.psv'
	dbe = '/data4/paper/paperoutput/false.psv'

	if auto != 'y':
		auto_update = raw_input('Auto-load immediately after finishing (y/n)?: ')
	else:
		auto_update == 'y'
	#iterates through directories, listing information about each one
	dirs_all = glob.glob(datanum)

	#removes duplicate entries from directory
	dirs = remove_duplicates(dirs_all, usrnm, pswd)

	dirs.sort()
	gen_paperdata(dirs, dbo, dbe)

	if auto_update == 'y':
		usrnm2 = raw_input('Input username with edit privileges: ')
		pswd2 = raw_input('Input password: ')
		table = 'paperdata'
		load_db_from_file(dbo, table, usrnm2, pswd2)
		sys.exit()

	fix_obs = raw_input('Update obsnums (y/n)?: ')
	if fix_obs == 'y':
		usrnm3 = raw_input('Input username with edit privileges: ')
		pswd3 = raw_input('Input password: ')
		obsnum_fixer(usrnm3, pswd3)

	return None

if __name__ == '__main__':
	auto = 'n'
	load_paperdata(auto)
