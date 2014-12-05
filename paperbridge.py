#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass
import os
import csv
import time
import load_paperdata
import subprocess
import smtplib
import shutil
import socket
import aipy as A
import hashlib
import base64

### Script to load infromation quickly from paperdistiller database into paperdata
### Queries paperdistiller for relevant information, loads paperdata with complete info

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

def gen_data_list(usrnm, pswd):
	#pulls all relevant information from full paperdistiller database
	connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdistiller', local_infile=True)

	# prepare a cursor object using cursor() method
	cursor = connection.cursor()

	# execute the SQL query using execute() method.
	cursor.execute('SELECT filename, obsnum, md5sum from file order by obsnum')
	result = cursor.fetchall()

	#gather all data to input into paperdata
	results = []
	for item in result:
		cursor.execute('''SELECT julian_date, pol, length, status, substring(julian_date, 4, 4) from observation where obsnum = %d '''%(int(item[1])))
		sec_results = cursor.fetchall()
		results.append(item + sec_results[0])	

	#results is list of tuples of filename[0], obsnum[1], md5sum[2], julian_date[3], pol[4], length[5], status[6], julian_day[7]

	#Make unique list of julian days
	cursor.execute('''SELECT substring(julian_date, 4, 4), count(*) from observation group by substring(julian_date, 4, 4)''')
	unique_jday = cursor.fetchall()

	#Make dict of the amount of each jday
	count_jday = {}
	for jday in unique_jday:
		count_jday.update({jday[0]:jday[1]})

	#Find all complete days in observation
	completed_days = []

	cursor.execute('''SELECT substring(julian_date, 4, 4), status, count(*) from observation group by substring(julian_date, 4, 4), status ''')
	completion = cursor.fetchall()
	for entry in completion:
		jday = entry[0]
		status = entry[1]
		count = entry[2]
		if status == 'COMPLETE':
			if count == count_jday[jday]:
				completed_days.append(jday)
			else:
				print jday + ' is incomplete'
		else:
			print jday + ' is incomplete'


	print completed_days
	#Remove all non-complete days
	for res in results[:]:
		if res[7] not in completed_days:
			results.remove(res)

	print len(results)
	#Close database and save changes
        cursor.close()
        connection.close()

	#Results should be list of days that have been completed for all files

	#pulls all relevant information from full paperdistiller database
        connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)

	# prepare a cursor object using cursor() method
        cursor = connection.cursor()

	#Create list of obsnums to check for duplicates-- only adds files that have not been compressed
	cursor.execute('''SELECT obsnum from paperdata where compressed = 1''')
	obs = cursor.fetchall()

	obsnums = []
	for obsnum in obs:
		obsnums.append(obsnum[0])

	#Close db connection
	cursor.close()
	connection.close()

	filenames = []
	for file in results:
		filenames.append(file[0])

	return [results, obsnums, filenames]

def gen_data_from_paperdistiller(results, obsnums, dbnum, dbe):
	host = 'folio'
	#results list of lists should contain path, obsnum, julian_date, polarization string, length of data, and julian_day
	for item in results:
		#Opens error logging file
		error_file = open(dbe,'ab')
		ewr = csv.writer(error_file, delimiter='|', lineterminator='\n', dialect='excel')
		#check for duplicate
		if item[1] in obsnums:
			err = [item, 'Obsnum already in paperdata']
			ewr.writerow(err)
			error_file.close()
			continue

		#Open file to write to
		data_file = open(dbnum,'ab')
		#create 'writer' object
		wr = csv.writer(data_file, delimiter='|', lineterminator='\n', dialect='excel')

		#indicates location of raw file (usually same directory as compressed)	
		if item[0].split('.')[-1] == 'uv':
			raw_path = item[0]
			raw_full_path = host + ':' + raw_path
			compr_path = item[0] + 'cRRE'
			compr_full_path = host + ':' + compr_path
		elif item[0].split('.')[-1] == 'uv/':
			raw_path = item[0][:-1]
			raw_full_path = host + ':' + raw_path
			compr_path = item[0][:-1] + 'cRRE'
			compr_full_path = host + ':' + compr_path
		else:
			err = [item, 'Not .uv file']
			ewr.writerow(err)
			error_file.close()
			data_file.close()
			continue

		#indicates size of compressed file MB
		#ASSUME DATABASE IS CORRECT
		compr_file = os.path.join(compr_path, 'visdata')
		if os.path.isfile(compr_file):
			compr_sz = round(float(sizeof_fmt(get_size(compr_path))), 1)
			compressed = 1
			edge = 0
		else:
			compr_sz = 0.0
			compressed = 0
			edge = 1
			err = [item, 'No compressed file -- possibly edge file']
			ewr.writerow(err)
		

		#indicates size of raw file in MB
		raw_file = os.path.join(raw_path, 'visdata')
		if os.path.isfile(raw_file):
			raw_sz = round(float(sizeof_fmt(get_size(raw_path))), 1)
		else:
			err = [item, 'No .uv file']
			ewr.writerow(err)
			error_file.close()
			continue

	        #allows uv access
		try:
		        uv = A.miriad.UV(raw_path)
		except:
			err = [item, 'Cannot access .uv file']
			ewr.writerow(err)
			error_file.close()
			data_file.close()
			continue	

		#indicates julian date
		jdate = round(float(item[3]), 5)

		#indicates julian day
		jday = int(item[7])	

		#indicates set of data used
		if jdate < 2456100:
			era = 32
		elif jdate < 2456400:
			era = 64
		else:
			era = 128

		#indicates type of file in era
		era_type = 'NULL'

		#assign letters to each polarization
		polarization = item[4]

		#indicates length of information in file
		length = round(float(item[5]),5)

		#indicates obsnum
		obsnum = int(item[1])

		#indicates md5sum -- SHOULD BE GENERATED BEFORE COMPRESSION
		if item[2] == 'NULL' or item[2] is None:
			try:
				md5 = md5sum(raw_path)
			except:
				md5 = 'NULL'
		else:
			md5 = item[2]

		#location of calibrate files
		cal_location = 'NULL'

		#shows location of raw data on tape
		tape_location = 'NULL'

		#variable indicating if all files have been successfully compressed in one day
		ready_to_tape = 0

		#indicates if all raw data is compressed, moved to tape, and the raw data can be deleted from folio
		delete_file = 0 

		#indicates when a file has been restored
		restore_history = 'NULL'

		#create list of important data and open csv file
		databs = [compr_full_path,era,era_type,obsnum,md5,jday,jdate,polarization,length,raw_full_path,cal_location,tape_location,compr_sz,raw_sz,compressed,edge,ready_to_tape,delete_file,restore_history]
		print databs 

		#write to csv file by item in list
		wr.writerow(databs)
		error_file.close()
		data_file.close()

	return None

def calculate_free_space(dir):
        #Calculates the free space left on input dir
        folio = subprocess.check_output(['du', '-bs', dir])
        #Amount of available bytes should be free_space

        #Do not surpass this amount ~1.2TiB
        max_space = 1319413953331

	total_space = 0
        for output in folio.split('\n'):
                subdir = output.split('\t')[-1]
                if subdir == dir:
                        total_space = int(output.split('\t')[0])
        free_space = max_space - total_space

        return free_space

def email_space(table):
        server = smtplib.SMTP('smtp.gmail.com', 587)
	server.ehlo()
	server.starttls()

        #Next, log in to the server
        server.login('paperfeed.paperdata@gmail.com', 'papercomesfrom1tree')

        #Send the mail
        msgs = '\nNot enough space for ' + table + ' on folio'

        server.sendmail('paperfeed.paperdata@gmail.com', 'immwa@sas.upenn.edu', msgs)
        server.sendmail('paperfeed.paperdata@gmail.com', 'jaguirre@sas.upenn.edu', msgs)
        server.sendmail('paperfeed.paperdata@gmail.com', 'saul.aryeh.kohn@gmail.com', msgs)

	server.quit()

        return None

def move_files(infile_list, outfile, move_data, usrnm, pswd):
        host = 'folio'

        #create file to log movement data       
        dbo = os.path.join(outfile, move_data)
        dbr = open(dbo,'wb')
        dbr.close()

        o_dict = {}
        for file in infile_list:
                zen = file.split('/')[-1]
		psa = file.split('.')[-4]

                subdir = os.path.join(psa,zen)
                outdir = os.path.join(outfile,psa)

                if not os.path.isdir(outdir):
                        os.mkidr(outdir)

                out = os.path.join(outfile,subdir)

                o_dict.update({file:out})

        #Load data into named database and table
        connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)
        cursor = connection.cursor()

        #Load into db
        for infile in infile_list:
                if infile.split('.')[-1] != 'uv':
                        print 'Invalid file type'
                        sys.exit()

                outfile = o_dict[infile]

                #Opens file to append to
                dbr = open(dbo, 'ab')
                wr = csv.writer(dbr, delimiter='|', lineterminator='\n', dialect='excel')

                #"moves" file
                try:
			inner = infile.split(':')[1]
                        shutil.move(inner, outfile)
                        wr.writerow([infile,outfile])
                        print infile, outfile
                        dbr.close()
                except:
                        dbr.close()
                        continue
                # execute the SQL query using execute() method, updates new location
                infile_path = infile
                outfile_path = host + ':' + o_dict[infile]
                if infile.split('.')[-1] == 'uv':
                        cursor.execute('''UPDATE paperdata set raw_path = '%s', ready_to_tape = 1 where raw_path = '%s' '''%(outfile_path, infile_path))

        print 'File(s) moved and updated'

        #Close database and save changes
        cursor.close()
        connection.commit()
        connection.close()

        return None

def paperbridge(auto):
	#User input information
	if auto != 'y':
		usrnm = raw_input('Username: ')
		pswd = getpass.getpass('Password: ')

		auto_load = raw_input('Automatically load into paperdata? (y/n): ')

	else:
		usrnm = 'jaguirre'
		pswd = base64.b64decode('amFndWlycmU2OTE5')
		auto_load = 'y'

	time_date = time.strftime("%d-%m-%Y_%H:%M:%S")
	dbnum = '/data4/paper/paperdistiller_output/paperdistiller_output_%s.psv'%(time_date)
	dbe = '/data4/paper/paperdistiller_output/paperdistiller_error_%s.psv'%(time_date)

	#Calculate amount of space needed to move a day ~1.1TB
	required_space = 1112661213184

	#Amount of space free in directory
	dir = '/data4/paper/raw_to_tape/'
	free_space = calculate_free_space(dir)

	if free_space >= required_space:
		#Pull information from paperdistiler
		results, obsnums, filenames = gen_data_list(usrnm,pswd)

		#Generate data from info pulled
		gen_data_from_paperdistiller(results, obsnums, dbnum, dbe)

		#check if auto-loading
		if auto_load == 'y':
			#Load information into paperdata
			load_paperdata.load_db(dbnum, usrnm, pswd)
			#Update paperdata and move data
			move_data = 'moved_data_%s.psv'%(time_date)	
			outfile = '/data4/paper/raw_to_tape'
			move_files(filenames, outfile, move_data, usrnm, pswd)
		else:
			print '''Information logged into '%s' ''' %(dbnum)

	else:
		table = 'paperdistiller'
		email_space(table)
		if auto == 'y':
			time.sleep(14400)

	return None

if __name__ == '__main__':
	auto = 'n'
	paperbridge(auto)
