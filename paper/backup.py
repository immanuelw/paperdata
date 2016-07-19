'''
paper.backup

backups database into json files

author | Immanuel Washington

Functions
---------
backup | backs up database
'''
from __future__ import print_function
import os
import time
import paper as ppdata
from paper.data import dbi as pdbi
from paper.distiller import dbi as ddbi
from paper.ganglia import dbi as pyg

def backup_db(db):
    '''
    backups database by loading into json files, named by timestamp

    Parameters
    ----------
    db | str: name of database
    '''
    if db == 'paperdata':
        xdbi = pdbi
    elif db == 'paperdistiller':
        xdbi = ddbi
    elif db == 'ganglia':
        xdbi = pyg

    meta = xdbi.Base.metadata
    dbi = xdbi.DataBaseInterface()

    timestamp = int(time.time())
    backup_dir = os.path.join(ppdata.root_dir, 'data/{db}/{timestamp}'.format(db=db, timestamp=timestamp))
    if not os.path.isdir(backup_dir):
        os.mkdir(backup_dir)

    tables = {table.name: table.primary_key.columns.keys()[0] for table in meta.tables.values()}

    with dbi.session_scope() as s:
        print(timestamp)
        for table, primary_key in tables.items():
            db_file = '{table}_{timestamp}.json'.format(table=table.lower(), timestamp=timestamp)
            backup_path = os.path.join(backup_dir, db_file)
            print(db_file)

            DB_table = getattr(xdbi, table)
            DB_dump = s.query(DB_table).order_by(getattr(DB_table, primary_key).asc())
            ppdata.json_data(backup_path, DB_dump)
            print('Table data backup saved')

if __name__ == '__main__':
    print('This is just a module')
