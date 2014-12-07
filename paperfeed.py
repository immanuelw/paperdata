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
import shutil

### Script to load paperdistiller with files from the paperfeed table
### Checks /data4 for space, moves entire days of data, then loads into paperdistiller

### Author: Immanuel Washington
### Date: 11-23-14

def calculate_free_space(dir):
        #Calculates the free space left on input dir
        folio = subprocess.check_output(['du', '-bs', dir])
        #Amount of available bytes should be free_space

        #Do not surpass this amount ~3.1TiB
        max_space = 3408486046105

	total_space = 0
        for output in folio.split('\n'):
                subdir = output.split('\t')[-1]
                if subdir == dir:
                        total_space = int(output.split('\t')[0])
        free_space = max_space - total_space

        return free_space

def find_data(usrnm, pswd):
	connection = MySQLdb.connect (host = 'shredder', user = usrnm, passwd = pswd, db = 'paperdata', local_infile=True)
	cursor = connection.cursor()

	cursor.execute('''SELECT julian_day, moved from paperfeed where moved = 0 group by julian_day, moved''')
	jdays_to_be_moved = cursor.fetchall()
	#NEED HIGHER LEVEL CHECK TO MAKE SURE EACH DAY IS COMPLETE
	jday = int(jdays_to_be_moved[0][0])

	cursor.execute('''SELECT raw_path, julian_day from paperfeed where moved = 0 and julian_day = %d'''%(jday))
	results = cursor.fetchall()

	#Close database
	cursor.close()
	connection.close()

	file_paths = []
	for item in results:
		file_paths.append(item[0])

	return file_paths

def move_files(infile_list, outfile, move_data, usrnm, pswd):
	host = 'folio'

        #create file to log movement data       
        dbo = os.path.join('/data4/paper/feed', move_data)
        dbr = open(dbo,'wb')
        dbr.close()

        o_dict = {}
        for file in infile_list:
                zen = file.split('/')[-1]
		psa = file.split('.')[-4]

                subdir = os.path.join(psa,zen)
                outdir = os.path.join(outfile,psa)

                if not os.path.isdir(outdir):
                        os.mkdir(outdir)

                out = os.path.join(outfile,subdir)

                o_dict.update({file:out})

	#List of files added
	outfile_list = []

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
                        cursor.execute('''UPDATE paperfeed set raw_path = '%s', moved = 1 where raw_path = '%s' '''%(outfile_path, infile_path))
		outfile_list.append(outfile_path.split(':')[1])

        print 'File(s) moved and updated'

        #Close database and save changes
        cursor.close()
        connection.commit()
        connection.close()


        return outfile_list

def email_paperfeed(files):
        server = smtplib.SMTP('smtp.gmail.com', 587)
	server.ehlo()
        server.starttls()

        #Next, log in to the server
        server.login('paperfeed.paperdata@gmail.com', 'papercomesfrom1tree')

        msgs = ''
        #Send the mail
        for file in files:
		msg = '\n' + file + ' is being moved.\n'
                msgs = msgs + msg

        server.sendmail('paperfeed.paperdata@gmail.com', 'immwa@sas.upenn.edu', msgs)
        server.sendmail('paperfeed.paperdata@gmail.com', 'jaguirre@sas.upenn.edu', msgs)
        server.sendmail('paperfeed.paperdata@gmail.com', 'saul.aryeh.kohn@gmail.com', msgs)
	server.sendmail('paperfeed.paperdata@gmail.com', 'jacobsda@sas.upenn.edu', msgs)

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
	server.sendmail('paperfeed.paperdata@gmail.com', 'jacobsda@sas.upenn.edu', msgs)

	server.quit()

        return None

def paperfeed(auto):
	#Create output file
	time_date = time.strftime("%d-%m-%Y_%H:%M:%S")
        move_data = 'moved_data_%s.psv'%(time_date)

	#Credentials
	if auto != 'y':
		usrnm = raw_input('Username: ')
        	pswd = getpass.getpass('Password: ')
	else:
		usrnm = 'immwa'
		pswd = 'immwa3978'

	#Checks all filesystems
	dir = '/data4/paper/feed/' #CHANGE WHEN KNOW WHERE DATA USUALLY IS STORED
	free_space = calculate_free_space(dir)

	#Minimum amount of space to move a day ~3.1TiB
	required_space = 1112373311360

	#Move if there is enough free space
	if free_space >= required_space:
		#FIND DATA
		infile_list = find_data(usrnm, pswd)
		#create directory to output to
		#output_subdir = infile_list[0].split('/')[-2]
		#outfile = os.path.join('/data4/paper/feed/', output_subdir)
		outfile = '/data4/paper/feed/'
		#MOVE DATA AND UPDATE PAPERFEED TABLE THAT FILES HAVE BEEN MOVED, AND THEIR NEW PATHS
		outfile_list = move_files(infile_list, outfile, move_data, usrnm, pswd)
		#EMAIL PEOPLE THAT DATA IS BEING MOVED AND LOADED
		email_paperfeed(outfile_list)
		#ADD_OBSERVATIONS.PY ON LIST OF DATA IN NEW LOCATION
		outfile_dirs = []
		for outfiles in outfile_list:
			if outfiles.split('z')[0] not in outfile_dirs:
				outfile_dirs.append(outfiles.split('z')[0])
		for out_direc in outfile_dirs:
			out_dir = os.path.join(out_direc,'zen.*.uv')
			add_obs = 'add_observations.py %s'%(out_dir)
			subprocess.call(add_obs, shell=True)
	else:
		table = 'paperfeed'
		email_space(table)
		if auto == 'y':
			time.sleep(21600)

	return None

if __name__ == '__main__':
	auto = 'n'
	paperfeed(auto)
