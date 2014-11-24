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
import time
import subprocess
import smtplib
import rename_uv
import load_paperrename
import load_paperfeed

### Script to load paperfeed with files from the paperrename table
### Checks /data4 for space, moves entire days of data, renames them the correct names, then loads into paperfeed

### Author: Immanuel Washington
### Date: 11-23-14


def calculate_free_space(dir):
	#Calculates the free space left on input dir
	folio = subprocess.check_output(['df', dir], shell=True)
	#/data4 should be filesystem
	#Amount of available bytes should be free_space

	for output in folio.split('\n'):
	        filesystem = output.split(' ')[-1]
	        if filesystem == '/data4':
			free_space = int(output.split(' ')[-4])
	return free_space

def email_paperrename(files)
	server = smtplib.SMTP('smtp.gmail.com', 587)

	#Next, log in to the server
	server.login('paperfeed.paperdata@gmail.com', 'papercomesfrom1tree')

	msgs = ''
	#Send the mail
	for file in files:
		# The /n separates the message from the headers
		jday = file[1]
		actual = file[2]
		expected = file[3]
		if actual == expected:
			msg = '\n' + jday + ' has expected # of ' + expected + ' files while having ' + actual + ' files.\n'
		else:
			msg = '\n' + jday + ' has expected # of ' + expected + ' files while having only ' + actual + ' files.'
	
		msgs = msgs + msg

	server.sendmail('paperfeed.paperdata@gmail.com', 'immwa@sas.upenn.edu', msgs)
	#server.sendmail('paperfeed.paperdata@gmail.com', 'jaguirre@sas.upenn.edu', msgs)

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

	cursor.execute('''SELECT julian_day, count(*) from paperrename where''')
	results = cursor.fetchall()

	for item in results:
		jday = int(item[0])
		actual = int(item[1])
	cursor.execute('''UPDATE paperrename SET actual_amount = %d  WHERE julian_day = %d''' %(actual, jday))

        #Close and save database
        cursor.close()
	connection.commit()
        connection.close()

        return None

def move_files(infile_list, outfile, move_data, usrnm, pswd):
	host = socket.gethostname()

        #Directory of the infiles
        infile_dir = infile_list[0].split('z')[0]

        #create file to log movement data       
        dbo = os.path.join('/data4/paper/', move_data)
        dbr = open(dbo,'wb')
        dbr.close()

        o_dict = {}
        for file in infile_list:
                zen = file.split('/')[-1]
                out = host + ':' + os.path.join(outfile,zen)
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
                dbr = open(dbo, 'a')
                wr = csv.writer(dbr, dialect='excel')

                #"moves" file
                try:
			#scp infile, outfile
			os.popen('''scp -r %s %s''' %(infile, outfile))
                        wr.writerow([infile,outfile])
                        print infile, outfile
                        dbr.close()
                except:
                        dbr.close()
                        continue
                # execute the SQL query using execute() method, updates new location
                infile_path = infile
                outfile_path = outfile
                if infile.split('.')[-1] == 'uv':
                        cursor.execute('''UPDATE paperrename set raw_path = '%s', moved = 1 where raw_path = '%s' '''%(outfile_path, infile_path))

        print 'File(s) moved and updated'

        #Close database and save changes
        cursor.close()
        connection.commit()
        connection.close()

	outfile_list = []
	for path in o_dict.values():
		outfile_list.append(path.split(':')[1])

        return outfile_list

if __name__ == '__main__':
	#Create output file
	time_date = time.strftime("%d-%m-%Y_%H:%M:%S")
        move_data = 'moved_data_%s.csv'%(time_date)

	#Credentials
	usrnm = raw_input('Username: ')
        pswd = getpass.getpass('Password: ')


        #location of directory to move to
        datashift = '/data4/paper/file_renaming_test_output/'

        #Named file to input renaming error
        dbrn = '/data4/paper/file_renaming_test_output/128error.csv'

	#Files to temporarily store information about renamed files
	dbo = '/data2/home/immwa/scripts/paper_output/paperrename_out.csv'
        dbe = '/data2/home/immwa/scripts/paper_output/false_paperrename.csv'

	#Checks all filesystems
	dir = '/*'
	free_space = calculate_free_space(dir)

	#Amount of free space needed -- ~4.1TB
	required_space = 4402341478

	#Move if there is enough free space
	if free_space > required_space:
		#COPY FILES FROM 1 USB INTO FOLIO
		outfile_list = move_files(infile_list, outfile, move_data, usrnm, pswd)
		#RENAME FILES
		rename_uv.rename_uv(outfile_list, datashift, dbrn)
		#LOAD INTO PAPERRENAME
		new_dir = os.path,join(datashift, '*')
		dirs_all = glob.glob(new_dir)
		dirs = load_paperrename.remove_duplicates(dirs_all, usrnm, pswd)
		dirs.sort()
		load_paperrename.load_db(dirs, dbo, dbe)
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

		dbo2 = '/data2/home/immwa/scripts/paper_output/paperfeed_out.csv'
		dbe2 = '/data2/home/immwa/scripts/paper_output/false_paperfeed.csv'

		#removes duplicate entries from directory 
		dirs2 = load_paperfeed.remove_duplicates(completed_days, usrnm, pswd)
		dirs2.sort()
		load_paperfeed.gen_paperfeed(dirs2, dbo2, dbe2)

		usrnm2 = raw_input('Input username with edit privileges: ')
		pswd2 = raw_input('Input password: ')
		load_paperfeed.load_db(dbo2, usrnm2, pswd2)	