#!/usr/bin/python
# -*- coding: utf-8 -*-
# Create pyganglia tables

import pyganglia_dbi


### Script to create pyganglia database
### Instantiates tables

### Author: Immanuel Washington
### Date: 5-06-15

def create_db():
	dbi = pyganglia_dbi.DataBaseInterface()
	dbi.create_db()

if __name__ == '__main__':
	create_db()
