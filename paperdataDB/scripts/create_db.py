#!/usr/bin/python
# -*- coding: utf-8 -*-
# Create paperdata tables

import paperdata_dbi


### Script to create paperdata database
### Instantiates tables

### Author: Immanuel Washington
### Date: 5-06-15

def create_db():
	dbi = paperdata_dbi.DataBaseInterface()
	dbi.create_db()

if __name__ == '__main__':
	create_db()
