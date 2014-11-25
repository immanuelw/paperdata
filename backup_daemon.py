#!/usr/bin/python
# -*- coding: utf-8 -*-

import time
import paperbackup

### Script to automatically load files into paperdistiller database
### Runs each step of process through modules

### Author: Immanuel Washington
### Date: 11-23-14

if __name__ == '__main__':
	try:
		while True:
			usrnm = 'paperboy'
			pswd = 'paperboy'
			time_date = time.strftime("%d-%m-%Y_%H:%M:%S")
			paperbackup.paperbackup(time_date, usrnm, pswd)
			time.sleep(86400)
	except KeyboardInterrupt:
		pass
