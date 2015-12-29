#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

from __future__ import print_function
import sys
import os
import time
import glob
import socket
import aipy as A
import dbi as dev

### Script to load data from anywhere into paperfeed database
### Crawls folio or elsewhere and reads through .uv files to generate all field information
### DOES NOT MOVE ANY DATA

### Author: Immanuel Washington
### Date: 05-18-14

def gen_feed_data(host, full_path):
    #mostly file data
    host = host
    path = os.path.dirname(full_path)
    filename = os.path.basename(full_path)

    #allows uv access
    try:
        uv = A.miriad.UV(full_path)
    except:
        return None

    #indicates julian date
    julian_date = round(uv['time'], 5)
    julian_day = int(str(julian_date)[3:7])

    ready_to_move = False
    moved_to_distill = False

    timestamp = int(time.time())

    feed_data = {'host':host,
                'path':path,
                'filename':filename,
                'full_path':full_path,
                'julian_day':julian_day,
                'ready_to_move':ready_to_move,
                'moved_to_distill':moved_to_distill,
                'timestamp':timestamp}

    action = 'add by feed'
    table = 'feed'
    identifier = full_path
    log_data = {'action':action,
                'table':table,
                'identifier':identifier,
                'timestamp':timestamp}

    return feed_data, log_data

def dupe_check(input_host, input_paths):
    #checks for files already in feed table
    dbi = dev.DataBaseInterface()
    s = dbi.Session()
    table = getattr(dev, 'Feed')
    FEEDs = s.query(table).all()
    s.close()
    #all files on same host
    filenames = tuple(os.path.join(getattr(FEED, 'path'), getattr(FEED, 'filename')) for FEED in FEEDs if getattr(FEED, 'host') == input_host)

    #for each input file, check if in filenames
    unique_paths = tuple(input_path for input_path in input_paths if input_path not in filenames)
        
    return unique_paths

def add_feeds_to_db(input_host, input_paths):
    dbi = dev.DataBaseInterface()
    for source in input_paths:
        feed_data, log_data = gen_feed_data(input_host, source)
        dbi.add_to_table('feed', feed_data)
        dbi.add_to_table('log', log_data)

    return None

if __name__ == '__main__':
    named_host = socket.gethostname()
    if len(sys.argv) == 2:
        input_host = sys.argv[1].split(':')[0]
        if input_host == sys.argv[1]:
            print('Needs host')
            sys.exit()
        input_paths = sys.argv[1].split(':')[1]
    elif len(sys.argv) == 3:
        input_host = sys.argv[1]
        input_paths = sys.argv[2]
    else:
        input_host = raw_input('Source directory host: ')
        input_paths = raw_input('Source directory path: ')

    if named_host == input_host:
        input_paths = glob.glob(input_paths)
    else:
        ssh = dev.login_ssh(input_host)
        input_paths = raw_input('Source directory path: ')
        _, path_out, _ = ssh.exec_command('ls -d {input_paths}'.format(input_paths=input_paths))
        input_paths = path_out.read().split('\n')[:-1]
        ssh.close()

    output_host = 'folio'
    feed_output = '/data4/paper/feed/'
    input_paths = dupe_check(input_host, input_paths)
    add_feeds_to_db(input_host, input_paths)
