#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

# import the MySQLdb and sys modules
import paperdata_dbi as pdbi
from sqlalchemy import func
from sqlalchemy.sql import label
import move_files
import sys
import os
import time
import glob
import socket
import subprocess
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email import Encoders

### Script to load paperdistiller with files from the paperfeed table
### Checks /data4 for space, moves entire days of data, then loads into paperdistiller

### Author: Immanuel Washington
### Date: 11-23-14

def count_days():
	dbi = pdbi.DataBaseInterface()
	s = dbi.Session()
	count_FEEDs = s.query(pdbi.Feed.julian_day, label('count', func.count(pdbi.Feed.julian_day))).group_by(pdbi.Feed.julian_day).all()
	all_FEEDs = s.query(pdbi.Feed).all()
	s.close()
	good_days = tuple(FEED.julian_day for FEED in count_FEEDs if FEED.count==288 or FEED.count==72)
	to_move = tuple(FEED.full_path for FEED in all_FEEDs if FEED.julian_day in good_days)

	for full_path in to_move:
		FEED = dbi.get_feed(full_path)
		dbi.set_feed_ready(FEED.full_path, True)

	return None

def find_data():
	dbi = pdbi.DataBaseInterface()
	s = dbi.Session()
	FEEDs = s.query(pdbi.Feed).filter(pdbi.Feed.moved_to_distill==False).filter(pdbi.Feed.ready_to_move==True).all()
	s.close()

	feed_paths = tuple((FEED.host, os.path.join(FEED.path, FEED.filename)) for FEED in FEEDs if FEED.julian_Day==FEEDS[0].julian_day)

	return feed_paths

def email_paperfeed(files):
	server = smtplib.SMTP('smtp.gmail.com', 587)
	server.ehlo()
	server.starttls()

	#Next, log in to the server
	server.login('paperfeed.paperdata@gmail.com', 'papercomesfrom1tree')

	header = 'From: PAPERFeed <paperfeed.paperdata@gmail.com>\nSubject: FILES ARE BEING MOVED\n'
    msgs = header
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
	header = 'From: PAPERFeed <paperfeed.paperdata@gmail.com>\nSubject: NOT ENOUGH SPACE ON FOLIO\n'
    msgs = header + '\nNot enough space for ' + table + ' on folio'

	server.sendmail('paperfeed.paperdata@gmail.com', 'immwa@sas.upenn.edu', msgs)
	server.sendmail('paperfeed.paperdata@gmail.com', 'jaguirre@sas.upenn.edu', msgs)
	server.sendmail('paperfeed.paperdata@gmail.com', 'saul.aryeh.kohn@gmail.com', msgs)
	server.sendmail('paperfeed.paperdata@gmail.com', 'jacobsda@sas.upenn.edu', msgs)

	server.quit()

	return None

def feed_db:
	#Checks all filesystems
	direc = '/data4/paper/feed/' #CHANGE WHEN KNOW WHERE DATA USUALLY IS STORED
	free_space = psutil.disk_usage(direc).free

	#Minimum amount of space to move a day ~3.1TiB
	required_space = 1112373311360

	#Move if there is enough free space
	if free_space >= required_space:
		#FIND DATA
		infile_paths = find_data()
		input_host = infile_paths[0][0]
		input_paths = tuple(path[0] for path in infile_paths)
		#create directory to output to
		output_host = 'folio'
		output_dir = '/data4/paper/feed/'
		#MOVE DATA AND UPDATE PAPERFEED TABLE THAT FILES HAVE BEEN MOVED, AND THEIR NEW PATHS
		outfile_list = move_files.move_files(input_host, input_paths, output_host, output_dir)
		#EMAIL PEOPLE THAT DATA IS BEING MOVED AND LOADED
		email_paperfeed(input_paths)
		#ADD_OBSERVATIONS.PY ON LIST OF DATA IN NEW LOCATION
		outfile_dirs = []
		for outfiles in outfile_list:
			if outfiles.split('z')[0] not in outfile_dirs:
				outfile_dirs.append(outfiles.split('z')[0])
		for out_direc in outfile_dirs:
			out_dir = os.path.join(out_direc,'zen.*.uv')
			add_obs = 'add_observations.py {out_dir}'.format(out_dir=out_dir)
			subprocess.call(add_obs, shell=True)
	else:
		table = 'Feed'
		email_space(table)
		time.sleep(21600)

	return None

if __name__ == '__main__':
	feed_db()
