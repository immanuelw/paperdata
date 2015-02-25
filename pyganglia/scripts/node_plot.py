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
	plt.xticks(time_dict.keys(), time_dict.values())

	axarr[0].set_title(filename)

	colors = {0:'b', 1:'g', 2:'r', 3:'c', 4:'m', 5:'y', 6:'k', 7:'w'}
	lines = {0:'s', 1:'--', 2:'^', 3:''}

	for plot in axarr[:3]:
		go = plot.add_subplot(111)
		if plot == axarr[0]:
			go.ylabel('Ram values')
			datx = table_info['ram'].keys()
			daty = {}
			for value in table_info['ram'].values():
				#value is list of entries (ex: host, user_perc, sys_perc, etc.) -- so {0: (host, ...), 1:(...), ...}
				for num in range(len(value) - 1):
					mdaty = value[1:][num]
					if num in daty.keys():
						daty[num] += (mdaty,)
					else:
						daty[num] = (mdaty,)
			for key, value in daty:
				go.plot(datx, value, colors[key % 8] + lines[key % 4])
		elif plot == axarr[1]:
			go.ylabel('Iostat values')
			datx = table_info['iostat'].keys()
			for value in table_info['iostat'].values():
				for num in range(len(value) - 2):
					mdaty = value[2:][num]
					if num in daty.keys():
						daty[num] += (mdaty,)
					else:
						daty[num] = (mdaty,)
			for key, value in daty:
				go.plot(datx, value, colors[key % 8] + lines[key % 4])
		elif plot == axarr[2]:
			go.ylabel('CPU Percentages')
			datx = table_info['cpu'].keys()
			daty = {}
			for value in table_info['cpu'].values():
				for num in range(len(value) - 2):
					mdaty = value[2:][num]
					if num in daty.keys():
						daty[num] += (mdaty,)
					else:
						daty[num] = (mdaty,)
			for key, value in daty:
				go.plot(datx, value, colors[key % 8] + lines[key % 4])

	plt.xlabel('Stage')
	plt.grid(True)
	plt.savefig(filename.replace('.uv', '.png'))
	plt.show()

	return None

if __name__ == '__main__':
	auto = 'n'
	plot_nodes(auto)
