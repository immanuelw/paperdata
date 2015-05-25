#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

import pyganglia_dbi as pyg
from sqlalchemy import func
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
		plt.axvline(xdata=x_values, linestyle='--')

		plt.subplot(212)
		plt.scatter(*zip(*process_plot), 'r')
		plt.axvline(xdata=x_values, linestyle='--')


		plt.title(os.path.basename(uv_file) ' on ' + file_host)
		plt.xticks(x_values, STAGEs)
		plt.ylabel('Time Between stages')
		plt.xlabel('Stage')
		plt.grid(True)
		#plt.savefig(os.path.join('/data4/paper/monitor', uv_file.replace('.uv', '.png')))
		plt.show()
	
	s.close()
	return None

def plot_jd_vs_file():
	dbi = pyg.DataBaseInterface()
	s = dbi.Session()
	OBSs = s.query(dbi.Observation.julian_day, func.count(dbi.Observation.julian_day)).group_by(dbi.Observation.julian_day).all()
	s.close()
	jd_data = tuple((OBS[0], OBS[1]) for OBS in OBSs)

	plt.plot(*jd_data, 'r--')

	plt.title('Number of Files in each Julian Day')
	plt.ylabel('Number of Files')
	plt.xlabel('Julian date')
	plt.grid(True)
	#plt.savefig('~/jd_vs_file.png')
	plt.show()
	
	return None

def plot_jd_vs_gaps():
	dbi = pyg.DataBaseInterface()
	s = dbi.Session()
	OBSs = s.query(dbi.Observation.julian_day, func.count(dbi.Observation.julian_day)).group_by(dbi.Observation.julian_day).all()
	s.close()
	jd_data = tuple((OBS[0], 288 - OBS[1]) if OBS[0] = 128 else (OBS[0], 72 - OBS[1]) for OBS in OBSs)

	plt.plot(*jd_data, 'r--')

	plt.title('Number of Gaps in each Julian Day')
	plt.ylabel('Number of Gaps')
	plt.xlabel('Julian date')
	plt.grid(True)
	#plt.savefig('~/jd_vs_gaps.png')
	plt.show()
	
	return None

def table_jdate_vs_pol():
	dbi = pyg.DataBaseInterface()
	s = dbi.Session()
	OBSs = s.query(dbi.Observation).all()
	s.close()
	jdate_data = tuple((OBS.julian_date, OBS.polarization) for OBS in OBSs)

	POLs = ('xx', 'xy', 'yx', 'yy', 'all')
	pol_dict = dict(zip(POLs, y_values))
	jdate_plot = tuple((pol_dict[key], value) for key, value in jdate_data)
	y_values = np.arange(1, len(POLs) + 1, 1)

	plt.plot(*jdate_plot, 'r')
	plt.axhline(ydata=y_values-.5, linestyle='--')

	plt.title('Polarization of each Julian Date')
	plt.yticks(y_values, POLs)
	plt.ylabel('Polarization')
	plt.xlabel('Julian Date')
	plt.grid(True)
	#plt.savefig('~/jdate_vs_pol_table.png')
	plt.show()
	
	return None

def table_jdate_vs_file():
	dbi = pyg.DataBaseInterface()
	s = dbi.Session()
	FILEs = s.query(dbi.File).filter(dbi.File.filetype=='uv')
	OBSs = s.query(dbi.Observation).all()
	obsnums = tuple(OBS.obsnum for OBS in OBSs)
	file_data = []
	for obsnum in obsnums:
		FILE = s.query(dbi.File).filter(dbi.File.obsnum==obsnum).filter(dbi.File.filetype=='uv').first()
		OBS = s.query(dbi.Observation).filter(dbi.Observation.obsnum==obsnum).one()
		file_data.append((FILE.filename, str(OBS.julian_date) + OBS.polarization))

	s.close()
	file_data

	JDATEs = tuple(value for key, value in file_data)

	pol_dict = dict(zip(POLs, y_values))
	jdate_plot = tuple((pol_dict[key], value) for key, value in jdate_data)
	y_values = np.arange(1, len(POLs) + 1, 1)

	plt.plot(*jdate_plot, 'r')
	plt.axhline(ydata=y_values-.5, linestyle='--')

	plt.title('Polarization of each Julian Date')
	plt.xticks(x_values, POLs)
	plt.yticks(y_values, POLs)
	plt.ylabel('Polarization')
	plt.xlabel('Julian Date')
	plt.grid(True)
	#plt.savefig('~/jdate_vs_file_table.png')
	plt.show()
	
	return None

if __name__ == '__main__':
	if len(sys.argv) > 1:
		filenames = glob.glob(sys.argv[1])
	else:
		filenames = glob.glob(raw_input('Input filename to be plotted: '))
	plot_monitor(filenames)
	#plot_jd_vs_file()
	#table_jdate_vs_pol()
