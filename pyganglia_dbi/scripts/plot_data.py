#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

import pyganglia_dbi as pyg
import sys
import os
import glob
import socket
import time
import numpy as np
import matplotlib.pyplot as plt

### Script to plot the current status of compression nodes
### Reads in data files of each node and outputs relevant plots

### Author: Immanuel Washington
### Date: 05-20-15

def plot_monitor(filenames):
	dbi = pyg.DataBaseInterface()
	s = dbi.Session()
	host = socket.gethostname()
	STAGEs = ('NEW','UV_POT', 'UV', 'UVC', 'CLEAN_UV', 'UVCR', 'CLEAN_UVC', 'ACQUIRE_NEIGHBORS', 'UVCRE',
				'NPZ', 'UVCRR', 'NPZ_POT', 'CLEAN_UVCRE', 'UVCRRE', 'CLEAN_UVCRR', 'CLEAN_NPZ', 'CLEAN_NEIGHBORS',
				'UVCRRE_POT', 'CLEAN_UVCRRE', 'CLEAN_UVCR', 'COMPLETE')
	x_values = np.arange(1, len(STAGEs) + 1, 1)
	stage_dict = dict(zip(STAGEs, x_values))
	for uv_file in filenames:
		full_path = host + ':' + uv_file
		MONITORs = s.query(dbi.Monitor).filter(dbi.Monitor.full_path==full_path).all()
		file_host = s.query(dbi.Monitor).filter(dbi.Monitor.full_path==full_path).one().host
		status_data = tuple((MONITOR.status, MONITOR.del_time) for MONITOR in MONITORs if MONITOR.del_time is not -1)
		process_data = tuple((MONITOR.status, MONITOR.time_end-MONITOR.time_start) for MONITOR in MONITORs if MONITOR.del_time is -1)
		status_plot = tuple((stage_dict[key], value) for key, value in status_data)
		process_plot = tuple((stage_dict[key], value) for key, value in process_data)

		plt.figure(1)
		plt.subplot(211)
		plt.scatter(*zip(*status_plot), 'b')

		plt.subplot(212)
		plt.scatter(*zip(*process_plot), 'r')

		plt.title(os.path.basename(uv_file) ' on ' + file_host)
		plt.xticks(x_values, STAGEs)
		plt.ylabel('Time Between stages')
		plt.xlabel('Stage')
		plt.grid(True)
		#plt.savefig(os.path.join('/data4/paper/monitor', uv_file.replace('.uv', '.png')))
		plt.show()

		
	s.close()
	return None

if __name__ == '__main__':
	if len(sys.argv) > 1:
		filenames = glob.glob(sys.argv[1])
	else:
		filenames = glob.glob(raw_input('Input filename to be plotted: '))
	plot_monitor(filenames)
