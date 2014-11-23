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

### Script to load paperdistiller with files from the paperfeed table
### Checks /data4 for space, moves entire days of data, then loads into paperdistiller

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
                        cursor.execute('''UPDATE paperfeed set raw_path = '%s', moved = 1 where raw_path = '%s' '''%(outfile_path, infile_path))

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

	#Checks all filesystems
	dir = '/*'
	free_space = calculate_free_space(dir)

	#Amount of free space needed -- ~3.1TB
	required_space = 3328599654

	#Move if there is enough free space
	if free_space > required_space:
		#FIND DATA
		infile_list = find_data(usrnm, pswd)
		#create directory to output to
		output_subdir = infile_list[0].split('/')[-2]
		outfile = os.path.join('/data4/paper/', output_subdir)
		#MOVE DATA AND UPDATE PAPERFEED TABLE THAT FILES HAVE BEEN MOVED, AND THEIR NEW PATHS
		outfile_list = move_files(infile_list, outfile, move_data, usrnm, pswd)
		#ADD_OBSERVATIONS.PY ON LIST OF DATA IN NEW LOCATION
		for outfile in outfile_list:
			os.popen('''add_observations.py %s'''%(outfile)) 
		#AUTO_COMPRESS.PY?
