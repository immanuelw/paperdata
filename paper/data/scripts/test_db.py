'''
paper.data.scripts.test_db

runs tests for most relevant functions of paper pipeline

author | Immanuel Washington

Functions
---------
script_test | runs script functions on test uv files
'''
from __future__ import print_function
import os
import doctest
import glob
import shutil
from paper.data import dbi as pdbi
import add_files
import backup_db
import restore_db
import move_files
import delete_files

def script_test():
    '''
    runs tests of scripts
    '''
    print('instantiating database interface object...')
    dbi = pdbi.DataBaseInterface(configfile=os.path.expanduser('~/paperdata/test.cfg'))
    
    print('creating db...')
    dbi.create_db()

    print('finding files to test...')
    test_paths_str = os.path.expanduser('~/test_data/zen*.uv*')
    test_paths = glob.glob(test_paths_str)

    print('adding files to db...')
    source_host = 'folio'

    add_files.add_files(dbi, source_host, test_paths)
    add_files.update_obsnums(dbi)
    add_files.connect_observations(dbi)

    print('backing up db...')
    backup_db.paperbackup(dbi, db='papertest')

    print('dropping db...')
    dbi.drop_db(pdbi.Base)

    print('creating db again...')
    dbi.create_db()

    print('loading db...')
    restore_db(dbi, table='File')
    restore_db(dbi, table='Observation')
    add_files.update_obsnums(dbi)
    add_files.connect_observations(dbi)

    print('moving files...')
    #copy files first?
    dest_host = 'node16'
    dest_path = os.path.expanduser('~/test_data/')
    move_files.move_files(dbi, source_host, source_paths, dest_host, dest_path)

    print('deleting files...')
    source_host = dest_host
    dest_host = 'folio'
    del_dir = os.path.expanduser('~/test_data_2/')
    os.mkdir(dest_path)
    source_paths = delete_files.delete_check(source_host)
    delete_files.delete_files(dbi, source_host, source_paths, dest_host, del_dir)

    print('dropping db again...')
    dbi.drop_db()

    print('deleting backup file...')
    backup_list = sorted(glob.glob('/data4/paper/paperdata_backup/[0-9]*'), reverse=True)
    timestamp = int(backup_list[0].split('/')[-1])
    backup_file = '/data4/paper/paperdata_backup/{timestamp}/{table}_{timestamp}.json'.format(table=table, timestamp=timestamp)
    os.remove(backup_file)

    print('deleting copied files...')
    shutil.rmtree(del_dir)

    print('Script test Complete!')

if __name__ == '__main__':
    script_test()
