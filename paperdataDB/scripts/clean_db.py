#!/usr/bin/python
# -*- coding: utf-8 -*-

# import the MySQLdb and sys modules
import sys
import getpass
import os
import csv
import socket
import paperdata_dbi

def clean_db():
	dbi = paperdata_dbi.DataBaseInterface()
	s = dbi.Session()
	FILES = s.query(dbi.File).all()
	s.close()
	return None

if __name__ == '__main__':
	if len(sys.argv) == 2:
		input_host = sys.argv[1]
	else:
		input_host = raw_input('Source directory host: ')
	clean_db()
