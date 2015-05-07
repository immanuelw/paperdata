#!/usr/bin/python
# -*- coding: utf-8 -*-
# Create paperdata tables

import paperdata_dbi


### Script to drop paperdata database
### Instantiates tables

### Author: Immanuel Washington
### Date: 5-06-15

def drop_db():
	dbi = paperdata_dbi.DataBaseInterface()
	dbi.drop_db()

if __name__ == '__main__':
	drop_db()
