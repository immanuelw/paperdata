'''
paper.ganglia.ganglia_db

author | Immanuel Washington

Classes
-------
Filesystem | attributes of named table
Monitor | attributes of named table
Ram | attributes of named table
Iostat | attributes of named table
Cpu | attributes of named table
'''
from __future__ import print_function

class Filesystem(object):
    def __init__(self):
        self.table = 'Filesystem'
        self.db_list = ('host',
                        'system',
                        'total_space',
                        'used_space',
                        'free_space',
                        'percent_space',
                        'filesystem_id',
                        'timestamp')
        self.db_descr = {'host': {'type': 'VARCHAR(100)', 'default': 'None', 'key': 'No', 'description': 'system that us being monitored'},
                        'system': {'type': 'VARCHAR(100)', 'default': 'None',
                        'key': 'No', 'description': 'directory that is being searched for space'},
                        'total_space': {'type': 'BIGINT', 'default': 'None', 'key': 'No', 'description': 'total space in system in bytes'},
                        'used_space': {'type': 'BIGINT', 'default': 'None', 'key': 'No', 'description': 'used space in system in bytes'},
                        'free_space': {'type': 'BIGINT', 'default': 'None', 'key': 'No', 'description': 'free space in system in bytes'},
                        'percent_space': {'type': 'BIGINT', 'default': 'None', 'key': 'No', 'description': 'percent of used space in system'},
                        'filesystem_id': {'type': 'VARCHAR(36)', 'default': 'None', 'key': 'Primary', 'description': 'id'},
                        'timestamp': {'type': 'BIGINT', 'default': 'None', 'key': 'No', 'description': 'time entry was updated'}}

class Monitor(object):
    def __init__(self):
        self.table = 'Monitor'
        self.db_list = ('host',
                        'base_path',
                        'filename',
                        'source',
                        'status',
                        'full_stats',
                        'del_time',
                        'time_start',
                        'time_end',
                        'timestamp')
        self.db_descr = {'host': {'type': 'VARCHAR(100)', 'default': 'None',
                        'key': 'No', 'description': 'node that uv file is being compressed on'},
                        'base_path': {'type': 'VARCHAR(100)', 'default': 'None',
                        'key': 'No', 'description': 'directory that file is located in'},
                        'filename': {'type': 'VARCHAR(100)', 'default': 'None',
                        'key': 'No', 'description': 'name/path of uv file being compressed'},
                        'source': {'type': 'VARCHAR(200)', 'default': 'None',
                        'key': 'No', 'description': 'combination of host, path, and filename'},
                        'status': {'type': 'VARCHAR(100)', 'default': 'None',
                        'key': 'No', 'description': 'state of compression file is currently doing'},
                        'full_stats': {'type': 'VARCHAR(200)', 'default': 'None',
                        'key': 'Unique', 'combination of source and status which is a unique identifier for each file'},
                        'del_time': {'type': 'BIGINT', 'default': 'None',
                        'key': 'No', 'description': 'time taken to finish step -- status transition'},
                        'time_start': {'type': 'BIGINT', 'default': 'None',
                        'key': 'No', 'description': 'time process started as a integer -- process transition'},
                        'time_end': {'type': 'BIGINT', 'default': 'None',
                        'key': 'No', 'description': 'time process ended as an integer -- process transition'},
                        'timestamp': {'type': 'BIGINT', 'default': 'None',
                        'key': 'No', 'description': 'time entry was updated'}}

class Ram(object):
    def __init__(self):
        self.table = 'Ram'
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
                        'ram_id',
                        'timestamp')
        self.db_descr = {'host': {'type': 'VARCHAR(100)', 'default': 'None', 'key': 'No', 'description': 'system that is being monitored'},
                        'total': {'type': 'BIGINT', 'default': 'None', 'key': 'No', 'description': 'total ram'},
                        'used': {'type': 'BIGINT', 'default': 'None', 'key': 'No', 'description': 'used ram'},
                        'free': {'type': 'BIGINT', 'default': 'None', 'key': 'No', 'description': 'free ram'},
                        'shared': {'type': 'BIGINT', 'default': 'None', 'key': 'No', 'description': 'shared ram'},
                        'buffers': {'type': 'BIGINT', 'default': 'None', 'key': 'No', 'description': 'buffers'},
                        'cached': {'type': 'BIGINT', 'default': 'None', 'key': 'No', 'description': 'cached ram'},
                        'bc_used': {'type': 'BIGINT', 'default': 'None', 'key': 'No', 'description': ''},
                        'bc_free': {'type': 'BIGINT', 'default': 'None', 'key': 'No', 'description': ''},
                        'swap_total': {'type': 'BIGINT', 'default': 'None', 'key': 'No', 'description': ''},
                        'swap_used': {'type': 'BIGINT', 'default': 'None', 'key': 'No', 'description': ''},
                        'swap_free': {'type': 'BIGINT', 'default': 'None', 'key': 'No', 'description': ''},
                        'ram_id': {'type': 'VARCHAR(36)', 'default': 'None', 'key': 'Primary', 'description': 'id'},
                        'timestamp': {'type': 'BIGINT', 'default': 'None', 'key': 'No', 'description': 'time entry was updated'}}

class Iostat(object):
    def __init__(self):
        self.table = 'Iostat'
        self.db_list = ('host',
                        'device',
                        'tps',
                        'read_s',
                        'write_s',
                        'bl_reads',
                        'bl_writes',
                        'timestamp')
        self.db_descr = {'host': {'type': 'VARCHAR(100)', 'default': 'None', 'key': 'No', 'description': 'system that is being monitored'},
                        'device': {'type': 'VARCHAR(100)', 'default': 'None', 'key': 'No', 'description': ''},
                        'tps': {'type': 'DECIMAL(7,2)', 'default': 'None', 'key': 'No', 'description': ''},
                        'read_s': {'type': 'DECIMAL(7,2)', 'default': 'None', 'key': 'No', 'description': 'reads per second'},
                        'write_s': {'type': 'DECIMAL(7,2)', 'default': 'None', 'key': 'No', 'description': 'writes per second'},
                        'bl_reads': {'type': 'BIGINT', 'default': 'None', 'key': 'No', 'description': 'block reads'},
                        'bl_writes': {'type': 'BIGINT', 'default': 'None', 'key': 'No', 'description': 'block writes'},
                        'iostat_id': {'type': 'VARCHAR(36)', 'default': 'None', 'key': 'Primary', 'description': 'id'},
                        'timestamp': {'type': 'BIGINT', 'default': 'None', 'key': 'No', 'description': 'time entry was updated'}}

class Cpu(object):
    def __init__(self):
        self.table = 'Cpu'
        self.db_list = ('host',
                        'cpu',
                        'user_perc',
                        'sys_perc',
                        'iowait_perc',
                        'idle_perc',
                        'intr_s',
                        'timestamp')
        self.db_descr = {'host': {'type': 'VARCHAR(100)', 'default': 'None', 'key': 'No', 'description': 'system that is being monitored'},
                        'cpu': {'type': 'INT', 'default': 'None', 'key': 'No', 'description': 'number of cpu/processor being monitored'},
                        'user_perc': {'type': 'DECIMAL(5,2)', 'default': 'None',
                        'key': 'No', 'description': 'percent of cpu being used by user'},
                        'sys_perc': {'type': 'DECIMAL(5,2)', 'default': 'None',
                        'key': 'No', 'description': 'percent of cpu being used by system'},
                        'iowait_perc': {'type': 'DECIMAL(5,2)', 'default': 'None', 'key': 'No', 'description': 'percent of cpu waiting'},
                        'idle_perc': {'type': 'DECIMAL(5,2)', 'default': 'None', 'key': 'No', 'description': 'percent of cpu that is idle'},
                        'intr_s': {'type': 'INT', 'default': 'None', 'key': 'No', 'description': 'instructions (per second?)'},
                        'cpu_id': {'type': 'VARCHAR(36)', 'default': 'None', 'key': 'Primary', 'description': 'id'},
                        'timestamp': {'type': 'BIGINT', 'default': 'None', 'key': 'No', 'description': 'time entry was updated'}}

#dictionary of instantiated classes
instant_class = {'Filesystem': Filesystem(),
                'Monitor': Monitor(),
                'Ram': Ram(),
                'Iostat': Iostat(),
                'Cpu': Cpu()}
classes = instant_class.keys()
all_classes = instant_class.values()

#Only do things if running this script, not importing
if __name__ == '__main__':
    print('Not a script file, just a module')
