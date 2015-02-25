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

def data_parser(info_list, tab):
	var_class = pyg.instant_class[tab]

	db_dict = var_class.db_dict
	var_flo = var_class.var_flo
	var_str = var_class.var_str
	var_int = var_class.var_int
	table = var_class.table

	limit_query = pyg.fetch(info_list, db_dict, var_flo, var_str, var_int, table)
	output = pyg.dbsearch(limit_query)

	return output

def plot_nodes(auto):
	table_info = {}
	filename = sys.args[1]
	for tab in pyg.classes:
		data = {}
		if table in ['monitor_files']:
			info_list = [(field, pyg.SEARCH, pyg.EXACT, [filename]) for field in tab.db_list]
			output = data_parser(info_list, tab)
			for datum in output:
				data[datum[-1]] = datum[:-1]
			table_info[tab] = data
			time_min, time_max = min(data.keys()), max(data.keys())
			continue
		else:
			info_list = [(field, pyg.SEARCH, pyg.RANGE, [time_min, time_max]) for field in tab.db_list]

		output = data_parser(info_list, tab)
		for datum in output:
			data[datum[-1]] = datum[:-1]
		table_info[tab] = data

	time_dict = {}
	for time, file_info in table_info['monitor_files'].items():
		time_dict[time] = file_info[1]

	f, axarr = plt.subplots(3, sharex=True)
	axarr[0].plot(x, y)
	axarr[0].set_title(filename)
	axarr[1].plot(x, y)
	axarr[2].plot(x, y)

	figure_name = 'figure.pdf'

	#plt.figure(1)
	plt.xticks(time_dict.keys(), time_dict.values())
	plt.subplot(211)

	plt.subplot(212)
	plt.plot(data_x, data_y, 'r') #list/tuple of matching points
	plt.subplot(212)
	x_min = min(data_x)
	x_max = max(data_x)
	y_min = min(data_y)
	y_max = max(data_y)
	plt.xlabel('Stage')
	plt.ylabel('Node Information')
	plt.title(filename)
	plt.axis([x_min, x_max, y_min, y_max])
	plt.grid(True)
	plt.savefig(figure_name)
	plt.show()

	return None

if __name__ == '__main__':
	auto = 'n'
	plot_nodes(auto)
