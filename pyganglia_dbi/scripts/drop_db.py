#!/usr/bin/python
# -*- coding: utf-8 -*-
# Create pyganglia tables

import pyganglia_dbi


### Script to drop pyganglia database
### Instantiates tables

### Author: Immanuel Washington
### Date: 5-06-15

def drop_db():
	dbi = pyganglia_dbi.DataBaseInterface()
	dbi.drop_db()

if __name__ == '__main__':
	drop_db()
