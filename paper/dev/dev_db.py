#!/usr/bin/python
# -*- coding: utf-8 -*-
# Module to creation of db schema

### Author: Immanuel Washington
### Date: 05-17-14

from __future__ import print_function

NOSEARCH = 0
SEARCH = 1

MIN = 2
MAX = 3
EXACT = 4
RANGE = 5
LIST = 6
NONE = 7

options = {EXACT:'EXACT', MIN:'MIN', MAX:'MAX', RANGE:'RANGE', LIST:'LIST', NONE:'NONE'}
classes = ('File', 'Observation', 'Feed', 'Log', 'Rtp_File', 'Rtp_Observation', 'Rtp_Log')

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
                        'source_host',
                        'write_to_tape',
                        'delete_file',
                        'timestamp')
        self.db_descr = {'host':('VARCHAR(100)', 'None', 'No', 'host of file system that file is located on'),
                        'path':('VARCHAR(100)', 'None', 'No', 'directory that file is located in'),
                        'filename':('VARCHAR(100)', 'None', 'No', 'name of file (ex: zen.2446321.16617.uvcRRE)'),
                        'filetype':('VARCHAR(20)', 'None', 'No', 'filetype (ex: uv, uvcRRE, npz)'),
                        'full_path':('VARCHAR(200)', 'None', 'Primary',
                                        'combination of host, path, and filename which is a unique identifier for each file'),
                        'obsnum':('BIGINT', 'None', 'Foreign', 'observation number used to track files using integer'),
                        'filesize':('DECIMAL(7, 2)', 'None', 'No', 'size of file in megabytes'),
                        'md5sum':('INTEGER', 'None', 'No', '32-bit integer md5 checksum of file'),
                        'tape_index':('VARCHAR(100)', 'None', 'No', 'indexed location of file on tape'),
                        'source_host':('VARCHAR(100)', 'None', 'No', 'original source(host) of file'),
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
                        'full_path':('VARCHAR(200)', 'None', 'Primary',
                                        'combination of host, path, and filename which is a unique identifier for each file'),
                        'julian_day':('INTEGER', 'None', 'No',
                                        'the last 4 digits for any julian date to separate into days: ex:(2456601 -> 6601)'),
                        'ready_to_move':('BOOLEAN', 'None', 'No', 'boolean value indicated whether file is ready to be moved to distill'),
                        'moved_to_distill':('BOOLEAN', 'None', 'No', 'boolean value indicated whether file has been moved to distill yet'),
                        'timestamp':('BIGINT', 'None', 'No', 'time entry was last updated')}

class Log:
    def __init__(self):
        self.table = 'Log'
        self.db_list = ('action',
                        'table',
                        'identifier',
                        'timestamp')
        self.db_descr = {'action':('VARCHAR(100)', 'None', 'No', 'action taken by script'),
                        'table':('VARCHAR(100)', 'None', 'No', 'table script is acting on'),
                        'identifier':('VARCHAR(200)', 'None', 'No', 'primary key of item that was changed'),
                        'timestamp':('BIGINT', 'None', 'No', 'time action was taken')}

class Rtp_File:
    def __init__(self):
        self.table = 'Rtp_File'
        self.db_list = ('host',
                        'path',
                        'filename',
                        'filetype',
                        'full_path',
                        'obsnum',
                        'md5sum',
                        'transferred',
                        'julian_day',
                        'new_host',
                        'new_path',
                        'timestamp')
        self.db_descr = {'host':('VARCHAR(100)', 'None', 'No', 'host of file system that file is located on'),
                        'path':('VARCHAR(100)', 'None', 'No', 'directory that file is located in'),
                        'filename':('VARCHAR(100)', 'None', 'No', 'name of file (ex: zen.2446321.16617.uvcRRE)'),
                        'filetype':('VARCHAR(20)', 'None', 'No', 'filetype (ex: uv, uvcRRE, npz)'),
                        'full_path':('VARCHAR(200)', 'None', 'Primary',
                                        'combination of host, path, and filename which is a unique identifier for each file'),
                        'obsnum':('BIGINT', 'None', 'Foreign', 'observation number used to track files using integer'),
                        'transferred':('BOOLEAN', 'None', 'No', 'boolean value indicated whether file has bee copied to USDB'),
                        'md5sum':('INTEGER', 'None', 'No', '32-bit integer md5 checksum of file'),
                        'julian_day':('INTEGER', 'None', 'No',
                                        'the last 4 digits for any julian date to separate into days: ex:(2456601 -> 6601)'),
                        'new_host':('VARCHAR(100)', 'None', 'No', 'new source(host) of file'),
                        'new_path':('VARCHAR(100)', 'None', 'No', 'new path of file of new host'),
                        'timestamp':('BIGINT', 'None', 'No', 'time entry was last updated')}

class Rtp_Observation:
    def __init__(self):
        self.table = 'Rtp_Observation'
        self.db_list = ('obsnum',
                        'julian_date',
                        'polarization',
                        'julian_day',
                        'era',
                        'length',
                        'prev_obs',
                        'next_obs',
                        'timestamp')
        self.db_descr = {'obsnum':('BIGINT', 'None', 'Primary', 'rtp_observation number used to track files using integer'),
                        'julian_date':('DECIMAL(12, 5)', 'None', 'No', 'julian date of rtp_observation'),
                        'polarization':('VARCHAR(4)', 'None', 'No', 'polarization of rtp_observation'),
                        'julian_day':('INTEGER', 'None', 'No',
                                        'the last 4 digits for any julian date to separate into days: ex:(2456601 -> 6601)'),
                        'era':('INTEGER', 'None', 'No', 'era of rtp_observation taken: 32, 64, 128'),
                        'length':('DECIMAL(6, 5)', 'None', 'No', 'length of time data was taken for particular rtp_observation'),
                        'prev_obs':('BIGINT', 'None', 'Unique', 'rtp_observation number of previous rtp_observation'),
                        'next_obs':('BIGINT', 'None', 'Unique', 'rtp_observation number of next rtp_observation'),
                        'timestamp':('BIGINT', 'None', 'No', 'time entry was last updated')}

class Rtp_Log:
    def __init__(self):
        self.table = 'Rtp_Log'
        self.db_list = ('action',
                        'table',
                        'identifier',
                        'timestamp')
        self.db_descr = {'action':('VARCHAR(100)', 'None', 'No', 'action taken by script'),
                        'table':('VARCHAR(100)', 'None', 'No', 'table script is acting on'),
                        'identifier':('VARCHAR(200)', 'None', 'No', 'primary key of item that was changed'),
                        'timestamp':('BIGINT', 'None', 'No', 'time action was taken')}

#dictionary of instantiated classes
instant_class = {'File':File(), 'Observation':Observation(), 'Feed':Feed(), 'Log':Log(),
                'Rtp_File':Rtp_File(), 'Rtp_Observation':Rtp_Observation(), 'Rtp_Log':Rtp_Log()}
all_classes = (File(), Observation(), Feed(), Log(), Rtp_File(), Rtp_Observation(), Rtp_Log())

#Only do things if running this script, not importing
if __name__ == '__main__':
    print('Not a script file, just a module')
