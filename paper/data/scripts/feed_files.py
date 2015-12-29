'''
paper.data.scripts.feed_files

adds files to feed table in paperdata database

author | Immanuel Washington

Functions
---------
gen_feed_data | pulls relevant field information from uv file
dupe_check | checks to see any files to be added already exist in database
add_feeds_to_db | adds entries to feed table
add_feeds | parses list of files then adds them to paperdata database
'''
from __future__ import print_function
import os
import time
import uuid
import aipy as A
import paper as ppdata
from paper.data import dbi as pdbi, file_data

def gen_feed_data(host, path):
    '''
    generates data for feed table

    Parameters
    ----------
    host | str: system host
    path | str: path of uv* file

    Returns
    -------
    tuple:
        dict: feed values
        dict: log values
    '''
    try:
        uv = A.miriad.UV(path)
    except:
        return (None,) * 2

    base_path, filename = os.path.split(path)
    source = ':'.join((host, path))

    timestamp = int(time.time())

    feed_data = {'host': host,
                'base_path': base_path,
                'filename': filename,
                'source': source,
                'julian_day': int(uv['time']),
                'is_movable': False,
                'is_moved': False,
                'timestamp': timestamp}

    log_data = {'action': 'add by feed',
                'table': 'Feed',
                'identifier': source,
                'log_id': str(uuid.uuid4()),
                'timestamp': timestamp}

    return feed_data, log_data

def dupe_check(dbi, source_host, source_paths):
    '''
    checks for files already in feed table on the same host

    Parameters
    ----------
    dbi | object: database interface object
    source_host | str: file host
    source_paths | list[str]: file paths

    Returns
    -------
    list[str]: files not in feed table
    '''
    with dbi.session_scope() as s:
        table = pdbi.Feed
        FEEDs = s.query(table).filter(table.host == source_host).all()
        all_paths = tuple(os.path.join(FEED.base_path, FEED.filename) for FEED in FEEDs)

    unique_paths = tuple(source_path for source_path in source_paths if source_path not in all_paths)
        
    return unique_paths

def add_feeds_to_db(dbi, source_host, source_paths):
    '''
    adds feed file data to table

    Parameters
    ----------
    dbi | object: database interface object
    source_host | str: file host
    source_paths | list[str]: file paths
    '''
    with dbi.session_scope() as s:
        for source_path in source_paths:
            feed_data, log_data = gen_feed_data(source_host, source_path)
            dbi.add_entry_dict(s, 'Feed', feed_data)
            dbi.add_entry_dict(s, 'Log', log_data)

def add_feeds(dbi, source_host, source_paths):
    '''
    generates list of input files, check for duplicates, add information to database

    Parameters
    ----------
    dbi | object: database interface object
    source_host | str: file host
    source_paths | list[str]: list of file paths
    '''
    source_paths = dupe_check(dbi, source_host, source_paths)
    add_feeds_to_db(dbi, source_host, source_paths)

if __name__ == '__main__':
    source_host, source_paths = file_data.source_info()
    dbi = pdbi.DataBaseInterface()
    add_feeds(dbi, source_host, source_paths_str)
