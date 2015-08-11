#!/usr/bin/python
# -*- coding: utf-8 -*-
# Create paperdev table

import paperdev_dbi as pdbi
import sys

### Script to create paperdev database
### Instantiates tables

### Author: Immanuel Washington
### Date: 5-06-15

def create_table(table=None):
	dbi = pdbi.DataBaseInterface()
	if table is None:
		sys.exit()
	#table = dbi.Final_Product
	dbi.create_table(table)

if __name__ == '__main__':
	create_table()
