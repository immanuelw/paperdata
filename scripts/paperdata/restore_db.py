'''
scripts.paperdata.restore_db

restores paperdata database from backup json file

author | Immanuel Washington

Functions
---------
restore_db | converts json file into python dicts, then loads into paperdata database
'''
from __future__ import print_function
import sys
import argparse
import glob
import json
from paper.data import dbi as pdbi
import add_files
import sqlalchemy.exc

def restore_db(backup_file=None, table=None):
    '''
    loads backups from json into database

    Parameters
    ----------
    backup_file | str: name of backup file --defaults to None
    table | str: table name --defaults to None
    '''
    if table is None:
        return
    if backup_file is None:
        backup_list = sorted(glob.glob('/data4/paper/paperdata_backup/[0-9]*'), reverse=True)
        timestamp = int(backup_list[0].split('/')[-1])
        backup_file = '/data4/paper/paperdata_backup/{timestamp}/{table}_{timestamp}.json'.format(table=table, timestamp=timestamp)

    dbi = pdbi.DataBaseInterface()
    meta = pdbi.Base.metadata
    load_table = meta.tables[table]

    with dbi.session_scope() as s, open(backup, 'r') as backup_db:
        entry_list = json.load(backup_db)
        for entry_dict in entry_list:
            print(entry_dict.items())
            try:
                s.add(load_table(**entry_dict))
            except KeyboardInterrupt:
                raise
            except:
                print('Failed to load in entry')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Delete files from the database')
    parser.add_argument('--table', type=str, help='table to restore')
    #parser.add_argument('--file', type=str, help='backup file path')

    args = parser.parse_args()
    restore_db(table=args.table)
    #restore_db(s, table='File')

    dbi = pdbi.DataBaseInterface()
    with dbi.session_scope() as s:
        refresh.update_obsnums(s)
        refresh.connect_observations(s)
        #refresh.update_md5(s)
