'''
paper.data.scripts.backup_db

backups paperdata database into json file

author | Immanuel Washington

Functions
---------
json_data | dumps dictionaries to json file
paperbackup | backs up paperdata database
'''
from __future__ import print_function
import os
import time
import paper as ppdata
from paper.data import dbi as pdbi

def paperbackup(dbi, db):
    '''
    backups database by loading into json files, named by timestamp

    Parameters
    ----------
    dbi | object: database interface object
    db | str: name of directory to place backups into
    '''
    timestamp = int(time.time())
    backup_dir = '/data4/paper/{db}_backup/{timestamp}'.format(db=db, timestamp=timestamp)
    if not os.path.isdir(backup_dir):
        os.mkdir(backup_dir)

    #tables = ('Observation', 'File', 'Feed', 'Log')
    tables = ('Observation', 'File', 'Log')
    table_sorts = {'Observation': {'first': 'julian_date', 'second': 'polarization'},
                    'File': {'first': 'obsnum', 'second': 'filename'},
                    'Feed': {'first': 'julian_day', 'second': 'filename'},
                    'Log': {'first': 'timestamp', 'second': 'action'}}
    with dbi.session_scope() as s:
        print(timestamp)
        for table in tables:
            db_file = '{table}_{timestamp}.json'.format(table=table.lower(), timestamp=timestamp)
            backup_path = os.path.join(backup_dir, db_file)
            print(db_file)

            DB_table = getattr(pdbi, table)
            DB_dump = s.query(DB_table).order_by(getattr(DB_table, table_sorts[table]['first']).asc(),
                                                getattr(DB_table, table_sorts[table]['second']).asc())
            ppdata.json_data(backup_path, DB_dump)
            print('Table data backup saved')

if __name__ == '__main__':
    dbi = pdbi.DataBaseInterface()
    paperbackup(dbi, db='paperdata')
