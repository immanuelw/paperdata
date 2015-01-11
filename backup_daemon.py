#!/usr/bin/python
# -*- coding: utf-8 -*-

import time
import paperbackup
import getpass

### Script to automatically load files into paperdistiller database
### Runs each step of process through modules

### Author: Immanuel Washington
### Date: 11-23-14

if __name__ == '__main__':
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
