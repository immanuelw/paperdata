#!/usr/bin/python
# -*- coding: utf-8 -*-
# Create paperdev tables

import paperdev_dbi as pdbi


### Script to create paperdev database
### Instantiates tables

### Author: Immanuel Washington
### Date: 5-06-15

def create_db():
	dbi = pdbi.DataBaseInterface()
	dbi.create_db()

if __name__ == '__main__':
	create_db()
