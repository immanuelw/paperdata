'''
paper.ganglia.scripts.backup_db

backups ganglia database into json file

author | Immanuel Washington

Functions
---------
json_data | dumps dictionaries to json file
paperbackup | backs up ganglia database
'''
from __future__ import print_function
import os
import sys
import time
import paper as ppdata
from paper.ganglia import dbi as pyg

def paperbackup(dbi):
    '''
    backups database by loading into json files, named by timestamp

    Parameters
    ----------
    dbi | object: database interface object
    '''
    timestamp = int(time.time())
    backup_dir = os.path.join('/data4/paper/pyganglia_backup', str(timestamp))
    if not os.path.isdir(backup_dir):
        os.mkdir(backup_dir)

    tables = {'Filesystem': 'system', 'Monitor': 'filename', 'Iostat': 'device', 'Ram': 'total', 'Cpu': 'cpu'}
    table_sorts = {table_name: {'first': 'timestamp', 'second': 'host', 'third': third_sort} for table_name, third_sort in tables.keys()}
    with dbi.session_scope as s:
        print(timestamp)
        for table in tables.keys():
            db_file = '{table}_{timestamp}.json'.format(table=table.lower(), timestamp=timestamp)
            backup_path = os.path.join(backup_dir, db_file)
            print(db_file)

            DB_table = getattr(pyg, table)
            DB_dump = s.query(DB_table).order_by(getattr(DB_table, table_sorts[table]['first']).asc(),
                                                getattr(DB_table, table_sorts[table]['second']).asc(),
                                                getattr(DB_table, table_sorts[table]['third']).asc())
            ppdata.json_data(backup_path, DB_dump)
            print('Table data backup saved')

if __name__ == '__main__':
    dbi = pyg.DataBaseInterface()
    paperbackup(dbi)
