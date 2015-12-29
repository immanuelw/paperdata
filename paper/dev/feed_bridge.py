#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

from __future__ import print_function
import dbi as dev
from sqlalchemy import func
from sqlalchemy.sql import label
import move_files
import sys
import os
import time
import shutil
import socket
import subprocess
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email import Encoders

### Script to load paperdistiller with files from the paperfeed table
### Checks /data4 for space, moves entire days of data, then loads into paperdistiller

### Author: Immanuel Washington
### Date: 11-23-14

def set_feed(source, output_host, output_dir, moved_to_distill=True):
    dbi = dev.DataBaseInterface()
    FEED = dbi.get_entry('feed', source)
    dbi.set_entry(FEED, 'host', output_host)
    dbi.set_entry(FEED, 'path', output_dir)
    dbi.set_entry(FEED, 'moved_to_distill', moved_to_distill)
    return None

def move_feed_files(input_host, input_paths, output_host, output_dir):
    #different from move_files, adds to feed
    named_host = socket.gethostname()
    destination = ''.join((output_host, ':', output_dir))
    if named_host == input_host:
        for source in input_paths:
            move_files.rsync_copy(source, destination)
            set_feed(source, output_host, output_dir)
            shutil.rmtree(source)
    else:
        ssh = dev.login_ssh(output_host)
        for source in input_paths:
            rsync_copy_command = '''rsync -ac {source} {destination}'''.format(source=source, destination=destination)
            rsync_del_command = '''rm -r {source}'''.format(source=source)
            ssh.exec_command(rsync_copy_command)
            set_feed(source, output_host, output_dir)
            ssh.exec_command(rsync_del_command)
        ssh.close()

    print('Completed transfer')
    return None

def count_days():
    dbi = dev.DataBaseInterface()
    s = dbi.Session()
    table = getattr(dev, 'Feed')
    count_FEEDs = s.query(getattr(table, 'julian_day'), label('count', func.count(getattr(table, 'julian_day'))))\
                            .group_by(getattr(table, 'julian_day')).all()
    all_FEEDs = s.query(table).all()
    good_days = tuple(getattr(FEED, 'julian_day') for FEED in count_FEEDs if getattr(FEED, 'count') == 288 or getattr(FEED, 'count') == 72)
    to_move = tuple(getattr(FEED, 'full_path') for FEED in all_FEEDs if getattr(FEED, 'julian_day') in good_days)
    s.close()

    for full_path in to_move:
        FEED = dbi.get_entry('feed', source)
        dbi.set_entry(FEED, 'ready_to_move', True)

    return None

def find_data():
    dbi = dev.DataBaseInterface()
    s = dbi.Session()
    table = getattr(dev, 'Feed')
    FEEDs = s.query(table).filter(getattr(table, 'moved_to_distill') == False).filter(getattr(table, 'ready_to_move') == True).all()
    s.close()

    #only move one day at a time
    feed_host = FEEDs[0].host
    feed_day = FEEDs[0].julian_day
    feed_paths = tuple(os.path.join(getattr(FEED, 'path'), getattr(FEED, 'filename'))
                        for FEED in FEEDs if getattr(FEED, 'julian_day') == feed_day)
    feed_filenames = tuple(getattr(FEED, 'filename') for FEED in FEEDs if getattr(FEED, 'julian_day') == feed_day)

    return feed_paths, feed_host, feed_filenames

def email_paperfeed(files):
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.ehlo()
    server.starttls()

    #Next, log in to the server
    server.login('paperfeed.paper@gmail.com', 'papercomesfrom1tree')

    header = 'From: PAPERFeed <paperfeed.paper@gmail.com>\nSubject: FILES ARE BEING MOVED\n'
    msgs = header
    #Send the mail
    for filename in files:
        msgs = ''.join(msgs, '\n', filename, ' is being moved.\n')

    server.sendmail('paperfeed.paper@gmail.com', 'immwa@sas.upenn.edu', msgs)
    server.sendmail('paperfeed.paper@gmail.com', 'jaguirre@sas.upenn.edu', msgs)
    server.sendmail('paperfeed.paper@gmail.com', 'saul.aryeh.kohn@gmail.com', msgs)
    server.sendmail('paperfeed.paper@gmail.com', 'jacobsda@sas.upenn.edu', msgs)

    server.quit()

    return None

def feed_bridge():
    #Minimum amount of space to move a day ~3.1TiB
    required_space = 1112373311360
    output_dir = '/data4/paper/feed/' #CHANGE WHEN KNOW WHERE DATA USUALLY IS STORED

    #Move if there is enough free space
    if move_files.enough_space(required_space, output_dir):
        #check how many days are in each
        count_days()
        #FIND DATA
        input_paths, input_host, input_filenames = find_data()
        #pick directory to output to
        output_host = 'folio'
        #MOVE DATA AND UPDATE PAPERFEED TABLE THAT FILES HAVE BEEN MOVED, AND THEIR NEW PATHS
        move_feed_files(input_host, input_paths, output_host, output_dir)
        #EMAIL PEOPLE THAT DATA IS BEING MOVED AND LOADED
        email_paperfeed(input_paths)
        #ADD_OBSERVATIONS.PY ON LIST OF DATA IN NEW LOCATION
        out_dir = os.path.join(output_dir, 'zen.*.uv')
        add_obs = 'python /usr/global/paper/CanopyVirtualEnvs/PAPER_Distiller/bin/add_observations.py {out_dir}'.format(out_dir=out_dir)
        #shell = True because wildcards can't be done without it
        subprocess.call(add_obs, shell=True)
    else:
        table = 'Feed'
        move_files.email_space(table)
        time.sleep(21600)

    return None

if __name__ == '__main__':
    feed_bridge()
