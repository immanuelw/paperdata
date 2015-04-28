#!/usr/bin/python
# -*- coding: utf-8 -*-

import time
import paperbackup
import paperbridge
import paperjunk
import paperrename
import papermd5
import getpass

### Script to automatically use daemons
### Runs each step of process through modules

### Author: Immanuel Washington
### Date: 04-27-14
def backup():
	usrnm = raw_input('Username: ')
	pswd = getpass.getpass('Password: ')
	try:
		while True:
			time_date = time.strftime("%d-%m-%Y_%H:%M:%S")
			backup_file = '/data4/paper/paperdata_backup/%s/paperdata_backup_%s.psv' %(time_date, time_date)
			paperbackup.paperbackup(time_date, usrnm, pswd)
			paperbackup.email_backup(backup_file)
			time.sleep(86400)
	except KeyboardInterrupt:
		pass
	return None

def bridge():
	try:
		while True:
			auto = 'y'
			paperbridge.paperbridge(auto)
	except KeyboardInterrupt:
		pass
	return None

def junk():
	try:
		while True:
			auto = 'y'
			paperjunk.paperjunk(auto)
	except KeyboardInterrupt:
		pass
	return None

def rename():
	try:
		while True:
			auto = 'y'
			paperrename.paperename(auto)
	except KeyboardInterrupt:
		pass
	return None

def md5():
	try:
		while True:
			auto = 'y'
			papermd5.papermd5(auto)
			time.sleep(28800)
	except KeyboardInterrupt:
		pass
	return None

if __name__ == '__main__':
	daemon = raw_input('Which daemon [backup, bridge, junk, rename, md5]')
	if daemon in ('backup', 'bridge', 'junk', 'rename', 'md5'):
		if daemon == 'backup':
			backup()
		elif daemon == 'bridge':
			bridge()
		elif daemon == 'junk':
			junk()
		elif daemon == 'rename':
			rename()
		elif daemon == 'md5':
			md5()
