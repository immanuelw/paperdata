#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

import MySQLdb
import pyganglia as pyg
import sys
import os
import csv
import glob
import socket
import time
import subprocess
import numpy as np
import matplotlib.pyplot as plt

### Script to plot the current status of compression nodes
### Reads in data files of each node and outputs relevant plots

### Author: Immanuel Washington
### Date: 01-26-15

def data_parser(info_list):
	#Decide which table to search
	tab = raw_input('Search which table? -- [monitor_files, ram, iostat, cpu]: ')
	if tab not in pyg.classes:
		sys.exit()

	var_class = pyg.instant_class[tab]

	db_dict = var_class.db_dict
	var_flo = var_class.var_flo
	var_str = var_class.var_str
	var_int = var_class.var_int
	table = var_class.table

	limit_query = pyg.fetch(info_list, db_dict, var_flo, var_str, var_int, table) + ' limit 100'
	output = pyg.dbsearch(limit_query)
	data_x, data_y = tuple([item[0] for item in output]), tuple([item[1] for item in output])

	return [data_x, data_y]

def plot_nodes(auto):
	#info_list = [[],[]]
	data_x, data_y = data_parser(info_list)
	figure_name = 'figure.pdf'

	plt.plot(data_x, data_y, 'r') #list/tuple of matching points
	#x_min = 
	#x_max = 
	#y_min = 
	#y_max = 
	plt.xlabel('Time')
	plt.ylabel('Node Information')
	plt.title('Node Title')
	plt.axis([x_min, x_max, y_min, y_max])
	plt.grid(True)
	plt.savefig(figure_name)
	plt.show()

	return None

if __name__ == '__main__':
	auto = 'n'
	plot_nodes(auto)
