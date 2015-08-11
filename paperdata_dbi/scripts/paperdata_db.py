#!/usr/bin/python
# -*- coding: utf-8 -*-
# Module to creation of db schema

### Author: Immanuel Washington
### Date: 05-17-14

NOSEARCH = 0
SEARCH = 1

MIN = 2
MAX = 3
EXACT = 4
RANGE = 5
LIST = 6
NONE = 7

options = {EXACT:'EXACT', MIN:'MIN', MAX:'MAX', RANGE:'RANGE', LIST:'LIST', NONE:'NONE'}
classes = ('File', 'Observation', 'Feed')

class File:
	def __init__(self):
		self.table = 'File'
		self.db_list = ('host',
						'path',
						'filename',
						'filetype',
						'full_path',
						'obsnum',
						'filesize',
						'md5sum',
						'tape_index',
						'write_to_tape',
						'delete_file',
						'timestamp')
		self.db_descr = {'host':('VARCHAR(100)', 'None', 'No', 'host of file system that file is located on'),
						'path':('VARCHAR(100)', 'None', 'No', 'directory that file is located in'),
						'filename':('VARCHAR(100)', 'None', 'No', 'name of file (ex: zen.2446321.16617.uvcRRE)'),
						'filetype':('VARCHAR(20)', 'None', 'No', 'filetype (ex: uv, uvcRRE, npz)'),
						'full_path':('VARCHAR(200)', 'None', 'Unique',
										'combination of host, path, and filename which is a unique identifier for each file'),
						'obsnum':('BIGINT', 'None', 'Primary', 'observation number used to track files using integer'),
						'filesize':('DECIMAL(7, 2)', 'None', 'No', 'size of file in megabytes'),
						'md5sum':('INTEGER', 'None', 'No', '32-bit integer md5 checksum of file'),
						'tape_index':('VARCHAR(100)', 'None', 'No', 'indexed location of file on tape'),
						'write_to_tape':('BOOLEAN', 'None', 'No', 'boolean value indicated whether file needs to be written to tape'),
						'delete_file':('BOOLEAN', 'None', 'No', 'boolean value indicated whether file needs to be deleted from its host'),
						'timestamp':('BIGINT', 'None', 'No', 'time entry was last updated')}

class Observation:
	def __init__(self):
		self.table = 'Observation'
		self.db_list = ('obsnum',
						'julian_date',
						'polarization',
						'julian_day',
						'era',
						'era_type',
						'length',
						'time_start',
						'time_end',
						'delta_time',
						'prev_obs',
						'next_obs',
						'edge',
						'timestamp')
		self.db_descr = {'obsnum':('BIGINT', 'None', 'Primary', 'observation number used to track files using integer'),
						'julian_date':('DECIMAL(12, 5)', 'None', 'No', 'julian date of observation'),
						'polarization':('VARCHAR(4)', 'None', 'No', 'polarization of observation'),
						'julian_day':('INTEGER', 'None', 'No',
										'the last 4 digits for any julian date to separate into days: ex:(2456601 -> 6601)'),
						'era':('INTEGER', 'None', 'No', 'era of observation taken: 32, 64, 128'),
						'era_type':('VARCHAR(20)', 'None', 'No', 'type of observation taken: dual pol, etc.'),
						'length':('DECIMAL(6, 5)', 'None', 'No', 'length of time data was taken for particular observation'),
						'time_start':('DECIMAL(12, 5)', 'None', 'No', 'start time of observation'),
						'time_end':('DECIMAL(12, 5)', 'None', 'No', 'end time of observation'),
						'delta_time':('DECIMAL(12, 5)', 'None', 'No', 'time step of observation'),
						'prev_obs':('BIGINT', 'None', 'Unique', 'observation number of previous observation'),
						'next_obs':('BIGINT', 'None', 'Unique', 'observation number of next observation'),
						'edge':('BOOLEAN', 'None', 'No', 'boolean value indicating if observation at beginning/end of night or not'),
						'timestamp':('BIGINT', 'None', 'No', 'time entry was last updated')}

class Feed:
	def __init__(self):
		self.table = 'Feed'
		self.db_list = ('host',
						'path',
						'filename',
						'full_path',
						'julian_day',
						'ready_to_move',
						'moved_to_distill',
						'timestamp')
		self.db_descr = {'host':('VARCHAR(100)', 'None', 'No', 'host of file system that file is located on'),
						'path':('VARCHAR(100)', 'None', 'No', 'directory that file is located in'),
						'filename':('VARCHAR(100)', 'None', 'No', 'name of file (ex: zen.2446321.16617.uvcRRE)'),
						'full_path':('VARCHAR(200)', 'None', 'Unique',
										'combination of host, path, and filename which is a unique identifier for each file'),
						'julian_day':('INTEGER', 'None', 'No',
										'the last 4 digits for any julian date to separate into days: ex:(2456601 -> 6601)'),
						'ready_to_move':('BOOLEAN', 'None', 'No', 'boolean value indicated whether file is ready to be moved to distill'),
						'moved_to_distill':('BOOLEAN', 'None', 'No', 'boolean value indicated whether file has been moved to distill yet'),
						'timestamp':('BIGINT', 'None', 'No', 'time entry was last updated')}

#dictionary of instantiated classes
instant_class = {'File':File(), 'Observation':Observation(), 'Feed':Feed()}
all_classes = (File(), Observation(), Feed())

#Only do things if running this script, not importing
if __name__ == '__main__':
	print 'Not a script file, just a module'
