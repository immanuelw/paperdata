#!/usr/bin/python
# -*- coding: utf-8 -*-
# Module to allow easier finding of data in scripts 

### Author: Immanuel Washington
### Date: 05-17-15

import sys
import os

# Config variables

NOSEARCH = 0
SEARCH = 1

MIN = 2
MAX = 3
EXACT = 4
RANGE = 5
LIST = 6
NONE = 7

options = {EXACT:'EXACT', MIN:'MIN', MAX:'MAX', RANGE:'RANGE', LIST:'LIST', NONE:'NONE'}
classes = ('monitor_files', 'ram', 'iostat', 'cpu')

class monitor_files:
	def __init__(self):
		self.table = 'monitor_files'
		self.db_list = ('filename',
						'status',
						'del_time',
						'time_start',
						'time_end',
						'still_host',
						'time_date')
		self.db_descr = {'filename':('VARCHAR(255)', 'None', 'No', 'name/path of uv file being compressed'),
						'status':('VARCHAR(255)', 'None', 'No', 'state of compression file is currently doing'),
						'del_time':('BIGINT', 'None', 'No', 'time taken to finish step -- status transition'),
						'time_start':('BIGINT', 'None', 'No', 'time process started as a integer -- process transition'),
						'time_end':('BIGINT', 'None', 'No', 'time process ended as an integer -- process transition'),
						'still_host'('VARCHAR(255)', 'None', 'No', 'node that uv file is being compressed on'),
						'time_date':('DECIMAL(13,6)', 'None', 'No', 'time and date entry was updated'}

class ram:
	def __init__(self):
		self.table = 'ram'
		self.db_list = ('host',
						'total',
						'used',
						'free',
						'shared',
						'buffers',
						'cached',
						'bc_used',
						'bc_free',
						'swap_total',
						'swap_used',
						'swap_free',
						'time_date')
		self.db_descr = {'host':('VARCHAR(255)', 'None', 'No', 'system that is being monitored'),
						'total':('BIGINT', 'None', 'No', 'total ram'),
						'used':('BIGINT', 'None', 'No', 'used ram'),
						'free':('BIGINT', 'None', 'No', 'free ram'),
						'shared':('BIGINT', 'None', 'No', 'shared ram'),
						'buffers':('BIGINT', 'None', 'No', 'buffers'),
						'cached':('BIGINT', 'None', 'No', 'cached ram'),
						'bc_used':('BIGINT', 'None', 'No', ''),
						'bc_free':('BIGINT', 'None', 'No', ''),
						'swap_total':('BIGINT', 'None', 'No', ''),
						'swap_used':('BIGINT', 'None', 'No', ''),
						'swap_free':('BIGINT', 'None', 'No', ''),
						'time_date':('DECIMAL(13,6)', 'None', 'No', 'time and date entry was updated')}

class iostat:
	def __init__(self):
		self.table = 'iostat'
		self.db_list = ('host',
						'device',
						'tps',
						'read_s',
						'write_s',
						'bl_reads',
						'bl_writes',
						'time_date')
		self.db_descr = {'host':('VARCHAR(255)', 'None', 'No', 'system that is being monitored'),
						'device':('VARCHAR(100)', 'None', 'No', ''),
						'tps':('DECIMAL(7,2)', 'None', 'No', ''),
						'read_s':('DECIMAL(7,2)', 'None', 'No', 'reads per second'),
						'write_s':('DECIMAL(7,2)', 'None', 'No', 'writes per second'),
						'bl_reads':('BIGINT', 'None', 'No', 'block reads'),
						'bl_writes':('BIGINT', 'None', 'No', 'block writes'),
						'time_date':('DECIMAL(13,6)', 'None', 'No', 'time and date entry was updated')}

class cpu:
	def __init__(self):
		self.table = 'cpu'
		self.db_list = ('host',
						'cpu',
						'user_perc',
						'sys_perc',
						'iowait_perc',
						'idle_perc',
						'intr_s',
						'time_date')
		self.db_descr = {'host':('VARCHAR(255)', 'None', 'No', 'system that is being monitored'),
						'cpu':('INT', 'None', 'No', 'No', 'number of cpu/processor being monitored'),
						'user_perc':('DECIMAL(5,2)', 'None', 'No', 'percent of cpu being used by user)',
						'sys_perc':('DECIMAL(5,2)', 'None', 'No', 'percent of cpu being used by system'),
						'iowait_perc':('DECIMAL(5,2)', 'None', 'No', 'percent of cpu waiting'),
						'idle_perc':('DECIMAL(5,2)', 'None', 'No', 'percent of cpu that is idle'),
						'intr_s':('INT', 'None', 'No', 'instructions (per second?)'),
						'time_date':('DECIMAL(13,6)', 'None', 'No', 'time and date entry was updated')}

#dictionary of instantiated classes
instant_class = {'monitor_files':monitor_files(), 'ram':ram(), 'iostat':iostat(), 'cpu':cpu()}
all_classes = (monitor_files(), ram(), iostat(), cpu())

#Only do things if running this script, not importing
if __name__ == '__main__':
	print 'Not a script file, just a module'
