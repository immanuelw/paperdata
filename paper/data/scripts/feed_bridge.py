'''
paper.data.scripts.feed_bridge

adds files to the paperdistiller database and updates the feed table

author | Immanuel Washington

Functions
---------
set_feed_table | updates database with feed file status
move_feed_files | parses list of files then moves them
count_days | counts the amount of files in each julian day and updates feed table
find_data | finds files which can be added to paperdistiller
feed_bridge | finds files to move, adds to paperdistiller after move
'''
from __future__ import print_function
import os
import time
import shutil
import socket
import paper as ppdata
from paper.data import dbi as pdbi
import paper.memory as memory
import distill_files
import move_files
from sqlalchemy import func
from sqlalchemy.sql import label

def set_feed_table(s, dbi, source_host, source_path, dest_host, dest_path, is_moved=True):
    '''
    updates table for feed file

    Parameters
    ----------
    s | object: session object
    dbi | object: database interface object
    source_host | str: user host
    source_path | str: source file path
    dest_host | str: output host
    dest_path | str: output directory
    is_moved | bool: checks whether to move to paperdistiller --defaults to True
    '''
    source = ':'.join((source_host, source_path))
    FEED = dbi.get_entry(s, 'Feed', source)
    feed_dict = {'host': dest_host,
                'base_path': dest_path,
                'is_moved': is_moved,
                'timestamp': int(time.time())}
    dbi.set_entry_dict(s, FEED, feed_dict)

def move_feed_files(dbi, source_host, source_paths, dest_host, dest_path):
    '''
    moves files and adds to feed directory and table

    Parameters
    ----------
    dbi | object: database interface object
    source_host | str: file host
    source_paths | list[str]: file paths
    dest_host | str: output host
    dest_path | str: output directory
    '''
    #different from move_files, adds to feed
    named_host = socket.gethostname()
    destination = ':'.join((dest_host, dest_path))
    with dbi.session_scope() as s:
        if named_host == source_host:
            for source_path in source_paths:
                ppdata.rsync_copy(source_path, destination)
                set_feed_table(s, dbi, source_host, source_path, dest_host, dest_path)
                shutil.rmtree(source_path)
        else:
            with ppdata.ssh_scope(source_host) as ssh:
                for source_path in source_paths:
                    rsync_copy_command = '''rsync -ac {source_path} {destination}'''.format(source_path=source_path, destination=destination)
                    rsync_del_command = '''rm -r {source_path}'''.format(source_path=source_path)
                    ssh.exec_command(rsync_copy_command)
                    set_feed_table(s, dbi, source_host, source_path, dest_host, dest_path)
                    ssh.exec_command(rsync_del_command)

    print('Completed transfer')

def count_days(dbi):
    '''
    checks amount of days in feed table and sets to move if reach requirement

    Parameters
    ----------
    dbi | object: database interface object
    '''
    with dbi.session_scope() as s:
        table = pdbi.Feed
        count_FEEDs = s.query(table.julian_day, label('count', func.count(table.julian_day))).group_by(table.julian_day).all()
        all_FEEDs = s.query(table).all()
        good_days = tuple(FEED.julian_day for FEED in count_FEEDs if FEED.count in (72, 288))

        to_move = (FEED.source for FEED in all_FEEDs if FEED.julian_day in good_days)

        for source in to_move:
            FEED = dbi.get_entry(s, 'Feed', source)
            dbi.set_entry(s, FEED, 'is_movable', True)

def find_data(dbi):
    '''
    finds data to move from feed table
    moves only one day at a time

    Parameters
    ----------
    dbi | object: database interface object

    Returns
    -------
    tuple:
        str: file host
        list[str]: file paths to move
    '''
    with dbi.session_scope() as s:
        table = pdbi.Feed
        FEEDs = s.query(table).filter(table.is_moved == False).filter(table.is_movable == True).all()

        feed_host = FEEDs[0].host
        feed_paths = tuple(os.path.join(FEED.base_path, FEED.filename) for FEED in FEEDs if FEED.julian_day == FEEDs[0].julian_day)

    return feed_host, feed_paths

def feed_bridge(dbi):
    '''
    bridges feed and paperdistiller
    moves files and pulls relevant data to add to paperdistiller from feed

    Parameters
    ----------
    dbi | object: database interface object
    '''
    #Minimum amount of memory to move a day ~3.1TiB
    required_memory = 1112373311360
    dest_path = '/data4/paper/feed/' #CHANGE WHEN KNOW WHERE DATA USUALLY IS STORED

    #Move if there is enough free memory
    if memory.enough_memory(required_memory, dest_path):
        #check how many days are in each
        count_days(dbi)
        #FIND DATA
        source_host, source_paths = find_data(dbi)
        #pick directory to output to
        dest_host = 'folio'
        #MOVE DATA AND UPDATE PAPERFEED TABLE THAT FILES HAVE BEEN MOVED, AND THEIR NEW PATHS
        move_feed_files(dbi, source_host, source_paths, dest_host, dest_path)
        #ADD FILES TO PAPERDISTILLER ON LIST OF DATA IN NEW LOCATION
        dest_dir = os.path.join(dest_path, 'zen.*.uv')
        obs_paths = glob.glob(dest_dir)
        distill_files.add_files_to_distill(obs_paths)
    else:
        table = 'Feed'
        memory.email_memory(table)
        time.sleep(21600)

if __name__ == '__main__':
    dbi = pdbi.DataBaseInterface()
    feed_bridge(dbi)
