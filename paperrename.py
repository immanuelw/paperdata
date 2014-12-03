#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

# import the MySQLdb and sys modules
import MySQLdb
import sys
import getpass
import os
import csv
import glob
import socket
import shutil
import aipy as A
import time
import subprocess
import smtplib
import load_paperrename
import load_paperfeed

### Script to load paperfeed with files from the paperrename table
### Checks /data4 for space, moves entire days of data, renames them the correct names, then loads into paperfeed

### Author: Immanuel Washington
### Date: 11-23-14

def calculate_free_space(dir):
	#Calculates the free space left on input dir
	folio = subprocess.check_output(['du', '-bs', dir])
	#Amount of available bytes should be free_space

	#Do not surpass this amount ~1.1TiB
	max_space = 1209462790553

	total_space = 0
	for output in folio.split('\n'):
	        subdir = output.split('\t')[-1]
	        if subdir == dir:
			total_space = int(output.split('\t')[0])
	free_space = max_space - total_space

	return free_space

def email_paperrename(files):
	server = smtplib.SMTP('smtp.gmail.com', 587)
	server.ehlo()
        server.starttls()

	#Next, log in to the server
	server.login('paperfeed.paperdata@gmail.com', 'papercomesfrom1tree')

	msgs = ''
	#Send the mail
	for file in files:
		# The /n separates the message from the headers
		jday = str(file[1])
		actual = str(file[2])
		expected = str(file[3])
		if actual == expected:
			msg = '\n' + jday + ' has expected # of ' + expected + ' files while having ' + actual + ' files.\n'
		else:
			msg = '\n' + jday + ' has expected # of ' + expected + ' files while having only ' + actual + ' files.'
	
		msgs = msgs + msg

	server.sendmail('paperfeed.paperdata@gmail.com', 'immwa@sas.upenn.edu', msgs)
	server.sendmail('paperfeed.paperdata@gmail.com', 'jaguirre@sas.upenn.edu', msgs)
	server.sendmail('paperfeed.paperdata@gmail.com', 'saul.aryeh.kohn@gmail.com', msgs)
	
	server.quit()

	return None

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

def check_data(usrnm, pswd):
	connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)
	cursor = connection.cursor()

	cursor.execute('''SELECT raw_path, julian_day, actual_amount, expected_amount from paperrename group by julian_day order by julian_day''')
	results = cursor.fetchall()

	cursor.execute('''SELECT raw_path, julian_day, actual_amount, expected_amount from paperrename where moved = 0 and actual_amount = expected_amount order by julian_day''')
        complete_results = cursor.fetchall()

	#Close database
	cursor.close()
	connection.close()

	file_info = results
	complete_info = complete_results

	return [file_info, complete_info]

def update_paperrename(usrnm, pswd):
	connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)
        cursor = connection.cursor()

	cursor.execute('''SELECT julian_day, count(*) from paperrename group by julian_day''')
	results = cursor.fetchall()

	for item in results:
		jday = int(item[0])
		actual = int(item[1])
		cursor.execute('''UPDATE paperrename SET actual_amount = %d WHERE julian_day = %d''' %(actual, jday))

        #Close and save database
        cursor.close()
	connection.commit()
        connection.close()

        return None

def update_paperjunk(infile_list, usrnm, pswd):
        connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)
        cursor = connection.cursor()

	for infile in infile_list:
        	cursor.execute('''UPDATE paperjunk SET renamed = 1 where folio_path = '%s' ''' %(infile))

        #Close and save database
        cursor.close()
        connection.commit()
        connection.close()

        return None

def rename_uv(dirs, datashift, dbe):
        #Dummy variable
        count = 0

        #polarization dictionary
        pol_dict = {-5:'xx',-6:'yy',-7:'xy',-8:'yx'}

        for data_file in dirs:
                count += 1
                #create csv file to log bad files
                error_file = open(dbe, 'ab')
                ewr = csv.writer(error_file, dialect='excel')

                #Find size of file
                data_size = os.path.getsize(data_file)

                #check if file size is over 700MB, if not: skip
                if data_size > 3832908476:
                        filler_dir = '/data4/paper/test_rename/64R/*'
                        file_type = '.uv'
                elif data_size > 3188051868:
                        filler_dir = '/data4/paper/test_rename/128R/*'
                        file_type = '.uv'
		elif  data_size > 734003200:
			filler_dir = '/data4/paper/test_rename/64U/*'
                        file_type = '.uv'
                elif data_size < 734003200:
                        continue

                if not os.path.isdir('/data4/paper/rename/%d.uv' %(count)):
                        try:
                                os.makedirs('/data4/paper/rename/%d.uv' %(count))
                        except:
                                print 'Error creating new directory for %s' %(dir)
                                continue
                        try:
                                for item in glob.glob(filler_dir):
                                        shutil.copy(item, '/data4/paper/rename/%d.uv' %(count))
                        except:
                                print 'Error copying filler data with %s' %(dir)
                                continue

                #if over 100MB, copy over to folio/copy to new folder and rename
                try:
                        shutil.move(data_file, '/data4/paper/rename/%d.uv/visdata' %(count))
                except:
                        print 'Directory /data4/paper/rename/%d.uv/ doesnt exist' %(count)
                        continue

                #set string to location of new .uv file
                newUV = '/data4/paper/rename/%d.uv' %(count)

                #allows uv access
                try:
                        print 'Accessing uv...'
                        #Fixes random fatal error
                        uv = 0
                        uv = A.miriad.UV(newUV)
                        print 'uv Success'
                except:
                        item = [newUV, dir,'Cannot access .uv file']
                        ewr.writerow(item)
                        error_file.close()
                        print 'UV Error'
                        continue

                print 'jd...'
                #find Julian Date
                jdate_num = round(uv['time'], 5)
		jdate = '%.5f'%(jdate_num)

                #print 'nchan...'
                #nchan = uv['nchan']
                #if nchan > 1000:
                #        file_type = '.uv'
                #else:
                #        file_type = '.uvcRRE'

                print 'pol...'
                #assign letters to each polarization
                if uv['npol'] == 1:
                        pol = pol_dict[uv['pol']]

                        #create variable to indicate new directory
                        newdir = 'zen.' + jdate + '.' + pol + file_type
                        newfile = os.path.join(datashift, newdir)

                #if polarizations aren't separated
                if uv['npol'] == 4:
                        newdir = 'zen.' + jdate + file_type
                        newfile = os.path.join(datashift, newdir)

                print newfile

                #copy data from one file to the other directory
                try:
                        shutil.move(newUV,newfile)
                except:
                        item = [newfile, dir, '''Couldn't move file''']
                        ewr.writerow(item)
                        error_file.close()
			continue

        return None

def paperrename(auto):
	#Create output file
	time_date = time.strftime("%d-%m-%Y_%H:%M:%S")
        move_data = 'moved_data_%s.csv'%(time_date)

	#Credentials
	if auto != 'y':
		usrnm = raw_input('Username: ')
	        pswd = getpass.getpass('Password: ')

	else:
		usrnm = 'immwa'
		pswd = 'immwa3978'

        #location of directory to move to
        datashift = '/data4/paper/rename/'

        #Named file to input renaming error
        dbrn = '/data4/paper/rename/128error.csv'

	#Files to temporarily store information about renamed files
	dbo = '/data4/paper/rename/paperrename_out.csv'
        dbe = '/data4/paper/rename/false_paperrename.csv'

	#Checks output dir
	dir = '/data4/paper/rename/'
	free_space = calculate_free_space(dir)

	#Amount of free space needed -- one file of 3.6 GB ~10GB for safety
	required_space = 10737418240

	#Move if there is enough free space
	if free_space >= required_space:
		#GET LIST OF FILES
		
		junk_dir = '/data4/paper/junk/I000*'
		infile_list = glob.glob(junk_dir)
		print infile_list
		#RENAME FILES AND UPDATE PAPERJUNK
		rename_uv(infile_list, datashift, dbrn)
		update_paperjunk(infile_list, usrnm, pswd)
		
		#LOAD INTO PAPERRENAME
		new_dir = os.path.join(datashift, '*')
		dirs_all = glob.glob(new_dir)
		dirs = load_paperrename.remove_duplicates(dirs_all, usrnm, pswd)
		dirs.sort()
		load_paperrename.gen_paperrename(dirs, dbo, dbe)
		load_paperrename.load_db(dbo, usrnm, pswd)
		#UPDATE PAPERRENAME
		update_paperrename(usrnm, pswd)
		
		#SCAN PAPERRENAME
		files_info, complete_info = check_data(usrnm,pswd)
		#SEND EMAIL OF WHICH DAYS ARE COMPLETE AND WHICH DAYS ARE NOT COMPLETE
		email_paperrename(files_info)
		#ADD INTO PAPERFEED
		#make list of files to add to paperfeed
		completed_days = []
		for file in complete_info:
			completed_days.append(file[0])

		dbo2 = '/data4/paper/feed/paperfeed_out.csv'
		dbe2 = '/data4/paper/feed/false_paperfeed.csv'

		#removes duplicate entries from directory 
		dirs2 = load_paperfeed.remove_duplicates(completed_days, usrnm, pswd)
		dirs2.sort()
		load_paperfeed.gen_paperfeed(dirs2, dbo2, dbe2)

		if auto != 'y':
			usrnm2 = raw_input('Input username with edit privileges: ')
			pswd2 = raw_input('Input password: ')
		else:
			usrnm2 = 'immwa'
			pswd2 = 'immwa3978'

		load_paperfeed.load_db(dbo2, usrnm2, pswd2)
		
	else:
		table = 'paperrename'
		email_space(table)
		if auto == 'y':
			time.sleep(21600)

	return None

if __name__ == '__main__':
	auto = 'n'
	paperrename(auto)	
