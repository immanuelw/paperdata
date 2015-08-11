#!/usr/bin/python
# -*- coding: utf-8 -*-
# Create paperdev tables

import paperdev_dbi as pdbi

### Script to drop paperdev database
### Instantiates tables

### Author: Immanuel Washington
### Date: 5-06-15

def drop_db():
	dbi = pdbi.DataBaseInterface()
	dbi.drop_db()

if __name__ == '__main__':
	drop_db()
