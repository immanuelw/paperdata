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

def data_out(time_date):
	#Create output file
	time_day = time.strftime('%d-%m-%Y')
	host = socket.gethostname()
	folio_data = '/data4/paper/paperoutput/log_%s_%s.psv' %(host, time_day)
	print folio_data
	dbr = open(folio_data, 'wb')
	dbr.close()

	return folio_data

def data_parser(file):

	return [node_data_x, node_data_y]

def plot_nodes(auto):
	plt.plot(node_data_x, node_data_y, 'r') #list/tuple of matching points
	plt.xlabel('Time')
	plt.ylabel('Node Information')
	plt.title('Node Title')
	plt.axis([x_min, x_max, y_min, y_max])
	plt.grid(True)
	plt.show()

	return None

if __name__ == '__main__':
	auto = 'n'
	plot_nodes(auto)
