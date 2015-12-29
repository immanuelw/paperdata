'''
paper.data.data_db

author | Immanuel Washington

Classes
-------
File | attributes of named table
Observation | attributes of named table
Feed | attributes of named table
Log | attributes of named table
RTPFile | attributes of named table
RTPObservation | attributes of named table
RTPLog | attributes of named table
'''
from __future__ import print_function

class File(object):
    def __init__(self):
        self.table = 'File'
        self.db_list = ('host',
                        'base_path',
                        'filename',
                        'filetype',
                        'source',
                        'obsnum',
                        'filesize',
                        'md5sum',
                        'tape_index',
                        'init_host',
                        'is_tapeable',
                        'is_deletable',
                        'timestamp')
        self.db_descr = {'host': {'type': 'VARCHAR(100)', 'default': 'None',
                        'key': 'No', 'description': 'host of file system that file is located on'},
                        'base_path': {'type': 'VARCHAR(100)', 'default': 'None',
                        'key': 'No', 'description': 'directory that file is located in'},
                        'filename': {'type': 'VARCHAR(100)', 'default': 'None',
                        'key': 'No', 'description': 'name of file (ex: zen.2446321.16617.uvcRRE)'},
                        'filetype': {'type': 'VARCHAR(20)', 'default': 'None',
                        'key': 'No', 'description': 'filetype (ex: uv, uvcRRE, npz)'},
                        'source': {'type': 'VARCHAR(200)', 'default': 'None',
                        'key': 'Primary', 'description': 'combination of host, path, and filename which is a unique identifier for each file'},
                        'obsnum': {'type': 'BIGINT', 'default': 'None',
                        'key': 'Foreign', 'description': 'observation number used to track files using integer'},
                        'filesize': {'type': 'DECIMAL(7, 2)', 'default': 'None',
                        'key': 'No', 'description': 'size of file in megabytes'},
                        'md5sum': {'type': 'INTEGER', 'default': 'None',
                        'key': 'No', 'description': '32-bit integer md5 checksum of file'},
                        'tape_index': {'type': 'VARCHAR(100)', 'default': 'None',
                        'key': 'No', 'description': 'indexed location of file on tape'},
                        'init_host': {'type': 'VARCHAR(100)', 'default': 'None',
                        'key': 'No', 'description': 'original host of file'},
                        'is_tapeable': {'type': 'BOOLEAN', 'default': 'None',
                        'key': 'No', 'description': 'boolean value indicated whether file needs to be written to tape'},
                        'is_deletable': {'type': 'BOOLEAN', 'default': 'None',
                        'key': 'No', 'description': 'boolean value indicated whether file needs to be deleted from its host'},
                        'timestamp': {'type': 'BIGINT', 'default': 'None',
                        'key': 'No', 'description': 'time entry was last updated'}}

class Observation(object):
    def __init__(self):
        self.table = 'Observation'
        self.db_list = ('obsnum',
                        'julian_date',
                        'polarization',
                        'julian_day',
                        'lst',
                        'era',
                        'era_type',
                        'length',
                        'time_start',
                        'time_end',
                        'delta_time',
                        'prev_obs',
                        'next_obs',
                        'is_edge',
                        'timestamp')
        self.db_descr = {'obsnum': {'type': 'BIGINT', 'default': 'None',
                        'key': 'Primary', 'description': 'observation number used to track files using integer'},
                        'julian_date': {'type': 'DECIMAL(12, 5)', 'default': 'None',
                        'key': 'No', 'description': 'julian date of observation'},
                        'polarization': {'type': 'VARCHAR(4)', 'default': 'None',
                        'key': 'No', 'description': 'polarization of observation'},
                        'julian_day': {'type': 'INTEGER', 'default': 'None',
                        'key': 'No', 'description': 'integer value of julian date'},
                        'lst': {'type': 'DECIMAL(3, 1)', 'default': 'None',
                        'key': 'No', 'description': 'local sidereal time for south africa at julian date'},
                        'era': {'type': 'INTEGER', 'default': 'None',
                        'key': 'No', 'description': 'era of observation taken: 32, 64, 128'},
                        'era_type': {'type': 'VARCHAR(20)', 'default': 'None',
                        'key': 'No', 'description': 'type of observation taken: dual pol, etc.'},
                        'length': {'type': 'DECIMAL(6, 5)', 'default': 'None',
                        'key': 'No', 'description': 'length of time data was taken for particular observation'},
                        'time_start': {'type': 'DECIMAL(12, 5)', 'default': 'None',
                        'key': 'No', 'description': 'start time of observation'},
                        'time_end': {'type': 'DECIMAL(12, 5)', 'default': 'None',
                        'key': 'No', 'description': 'end time of observation'},
                        'delta_time': {'type': 'DECIMAL(12, 5)', 'default': 'None',
                        'key': 'No', 'description': 'time step of observation'},
                        'prev_obs': {'type': 'BIGINT', 'default': 'None',
                        'key': 'Unique', 'description': 'observation number of previous observation'},
                        'next_obs': {'type': 'BIGINT', 'default': 'None',
                        'key': 'Unique', 'description': 'observation number of next observation'},
                        'is_edge': {'type': 'BOOLEAN', 'default': 'None',
                        'key': 'No', 'description': 'boolean value indicating if observation at beginning/end of night or not'},
                        'timestamp': {'type': 'BIGINT', 'default': 'None',
                        'key': 'No', 'description': 'time entry was last updated'}}

class Feed(object):
    def __init__(self):
        self.table = 'Feed'
        self.db_list = ('host',
                        'base_path',
                        'filename',
                        'source',
                        'julian_day',
                        'is_movable',
                        'is_moved',
                        'timestamp')
        self.db_descr = {'host': {'type': 'VARCHAR(100)', 'default': 'None',
                        'key': 'No', 'description': 'host of file system that file is located on'},
                        'base_path': {'type': 'VARCHAR(100)', 'default': 'None',
                        'key': 'No', 'description': 'directory that file is located in'},
                        'filename': {'type': 'VARCHAR(100)', 'default': 'None',
                        'key': 'No', 'description': 'name of file (ex: zen.2446321.16617.uvcRRE)'},
                        'source': {'type': 'VARCHAR(200)', 'default': 'None',
                        'key': 'Primary', 'description': 'combination of host, path, and filename which is a unique identifier for each file'},
                        'julian_day': {'type': 'INTEGER', 'default': 'None',
                        'key': 'No', 'description': 'integer value of julian date'},
                        'is_movable': {'type': 'BOOLEAN', 'default': 'None',
                        'key': 'No', 'description': 'boolean value indicated whether file is ready to be moved to distill'},
                        'is_moved': {'type': 'BOOLEAN', 'default': 'None',
                        'key': 'No', 'description': 'boolean value indicated whether file has been moved to distill yet'},
                        'timestamp': {'type': 'BIGINT', 'default': 'None',
                        'key': 'No', 'description': 'time entry was last updated'}}

class Log(object):
    def __init__(self):
        self.table = 'Log'
        self.db_list = ('action',
                        'table',
                        'identifier',
                        'log_id',
                        'timestamp')
        self.db_descr = {'action': {'type': 'VARCHAR(100)', 'default': 'None',
                        'key': 'No', 'description': 'action taken by script'},
                        'table': {'type': 'VARCHAR(100)', 'default': 'None',
                        'key': 'No', 'description': 'table script is acting on'},
                        'identifier': {'type': 'VARCHAR(200)', 'default': 'None',
                        'key': 'No', 'description': 'key of item that was changed'},
                        'log_id': {'type': 'VARCHAR(36)', 'default': 'None',
                        'key': 'Primary', 'description': 'id of log'},
                        'timestamp': {'type': 'BIGINT', 'default': 'None',
                        'key': 'No', 'description': 'time action was taken'}}

class RTPFile(object):
    def __init__(self):
        self.table = 'RTPFile'
        self.db_list = ('host',
                        'base_path',
                        'filename',
                        'filetype',
                        'source',
                        'obsnum',
                        'md5sum',
                        'is_transferred',
                        'julian_day',
                        'new_host',
                        'new_path',
                        'timestamp')
        self.db_descr = {'host': {'type': 'VARCHAR(100)', 'default': 'None',
                        'key': 'No', 'description': 'host of file system that file is located on'},
                        'base_path': {'type': 'VARCHAR(100)', 'default': 'None',
                        'key': 'No', 'description': 'directory that file is located in'},
                        'filename': {'type': 'VARCHAR(100)', 'default': 'None',
                        'key': 'No', 'description': 'name of file (ex: zen.2446321.16617.uvcRRE)'},
                        'filetype': {'type': 'VARCHAR(20)', 'default': 'None',
                        'key': 'No', 'description': 'filetype (ex: uv, uvcRRE, npz)'},
                        'source': {'type': 'VARCHAR(200)', 'default': 'None',
                        'key': 'Primary', 'description': 'combination of host, path, and filename which is a unique identifier for each file'},
                        'obsnum': {'type': 'BIGINT', 'default': 'None',
                        'key': 'Foreign', 'description': 'observation number used to track files using integer'},
                        'is_transferred': {'type': 'BOOLEAN', 'default': 'None',
                        'key': 'No', 'description': 'boolean value indicated whether file has bee copied to USDB'},
                        'md5sum': {'type': 'INTEGER', 'default': 'None',
                        'key': 'No', 'description': '32-bit integer md5 checksum of file'},
                        'julian_day': {'type': 'INTEGER', 'default': 'None',
                        'key': 'No', 'description': 'integer value of julian date'},
                        'new_host': {'type': 'VARCHAR(100)', 'default': 'None',
                        'key': 'No', 'description': 'new source(host) of file'},
                        'new_path': {'type': 'VARCHAR(100)', 'default': 'None',
                        'key': 'No', 'description': 'new path of file of new host'},
                        'timestamp': {'type': 'BIGINT', 'default': 'None',
                        'key': 'No', 'description': 'time entry was last updated'}}

class RTPObservation(object):
    def __init__(self):
        self.table = 'RTPObservation'
        self.db_list = ('obsnum',
                        'julian_date',
                        'polarization',
                        'julian_day',
                        'era',
                        'length',
                        'prev_obs',
                        'next_obs',
                        'timestamp')
        self.db_descr = {'obsnum': {'type': 'BIGINT', 'default': 'None',
                        'key': 'Primary', 'description': 'RTPObservation number used to track files using integer'},
                        'julian_date': {'type': 'DECIMAL(12, 5)', 'default': 'None',
                        'key': 'No', 'description': 'julian date of RTPObservation'},
                        'polarization': {'type': 'VARCHAR(4)', 'default': 'None',
                        'key': 'No', 'description': 'polarization of RTPObservation'},
                        'julian_day': {'type': 'INTEGER', 'default': 'None',
                        'key': 'No', 'description': 'integer value of julian date'},
                        'era': {'type': 'INTEGER', 'default': 'None',
                        'key': 'No', 'description': 'era of RTPObservation taken: 32, 64, 128'},
                        'length': {'type': 'DECIMAL(6, 5)', 'default': 'None',
                        'key': 'No', 'description': 'length of time data was taken for particular RTPObservation'},
                        'prev_obs': {'type': 'BIGINT', 'default': 'None',
                        'key': 'Unique', 'description': 'RTPObservation number of previous RTPObservation'},
                        'next_obs': {'type': 'BIGINT', 'default': 'None',
                        'key': 'Unique', 'description': 'RTPObservation number of next RTPObservation'},
                        'timestamp': {'type': 'BIGINT', 'default': 'None',
                        'key': 'No', 'description': 'time entry was last updated'}}

class RTPLog(object):
    def __init__(self):
        self.table = 'RTPLog'
        self.db_list = ('action',
                        'table',
                        'identifier',
                        'log_id',
                        'timestamp')
        self.db_descr = {'action': {'type': 'VARCHAR(100)', 'default': 'None',
                        'key': 'No', 'description': 'action taken by script'},
                        'table': {'type': 'VARCHAR(100)', 'default': 'None',
                        'key': 'No', 'description': 'table script is acting on'},
                        'identifier': {'type': 'VARCHAR(200)', 'default': 'None',
                        'key': 'No', 'description': 'key of item that was changed'},
                        'log_id': {'type': 'VARCHAR(36)', 'default': 'None',
                        'key': 'Primary', 'description': 'id of log'},
                        'timestamp': {'type': 'BIGINT', 'default': 'None',
                        'key': 'No', 'description': 'time action was taken'}}

#dictionary of instantiated classes
instant_class = {'File': File(),
                'Observation': Observation(),
                'Feed': Feed(),
                'Log': Log(),
                'RTPFile': RTPFile(),
                'RTPObservation': RTPObservation(),
                'RTPLog': RTPLog()}
classes = instant_class.keys()
all_classes = instant_class.values()

#Only do things if running this script, not importing
if __name__ == '__main__':
    print('Not a script file, just a module')
