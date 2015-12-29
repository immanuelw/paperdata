#!/usr/bin/python
# -*- coding: utf-8 -*-
# Add files to paper

from __future__ import print_function
import sys
import time
import subprocess
import aipy as A
import glob
import socket
import os
import shutil
import psutil
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email import Encoders
import dbi as dev

### Script to move files and update paper database
### Move files and update db using dbi

### Author: Immanuel Washington
### Date: 5-06-15

def enough_space(required_space, space_path):
    free_space = psutil.disk_usage(space_path).free

    if required_space < free_space:
        return True

    return False

def email_space(table):
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.ehlo()
    server.starttls()

    #Next, log in to the server
    server.login('paperfeed.paper@gmail.com', 'papercomesfrom1tree')

    #Send the mail
    header = 'From: PAPERBridge <paperfeed.paper@gmail.com>\nSubject: NOT ENOUGH SPACE ON FOLIO\n'
    msgs = ''.join((header, '\nNot enough space for ', table, ' on folio'))

    server.sendmail('paperfeed.paper@gmail.com', 'immwa@sas.upenn.edu', msgs)
    server.sendmail('paperfeed.paper@gmail.com', 'jaguirre@sas.upenn.edu', msgs)
    server.sendmail('paperfeed.paper@gmail.com', 'saul.aryeh.kohn@gmail.com', msgs)
    server.sendmail('paperfeed.paper@gmail.com', 'jacobsda@sas.upenn.edu', msgs)

    server.quit()

    return None

def null_check(input_host, input_paths):
    dbi = dev.DataBaseInterface()
    s = dbi.Session()
    table = getattr(dev, 'File')
    FILEs = s.query(table).filter(getattr(table, 'host') == input_host).all()
    s.close()
    #all files on same host
    filenames = tuple(os.path.join(getattr(FILE, 'path'), getattr(FILE, 'filename')) for FILE in FILEs)

    #for each input file, check if in filenames
    nulls = tuple(input_path for input_path in input_paths if input_path not in filenames)
        
    if len(nulls) > 0:
        return False

    return True

def set_move_table(input_host, source, output_host, output_dir):
    #change in database
    dbi = dev.DataBaseInterface()
    action = 'move'
    table = 'file'
    full_path = ''.join((input_host, ':', source))
    timestamp = int(time.time())
    FILE = dbi.get_entry('file', full_path)
    dbi.set_entry(FILE, 'host', output_host)
    dbi.set_entry(FILE, 'path', output_dir)
    dbi.set_entry(FILE, 'timestamp', timestamp)
    identifier = getattr(FILE, 'full_path')
    log_data = {'action':action,
                'table':table,
                'identifier':identifier,
                'timestamp':timestamp}
    dbi.add_to_table('log', log_data)
    return None

def rsync_copy(source, destination):
    subprocess.check_output(['rsync', '-ac', source, destination])
    return None

def move_files(input_host, input_paths, output_host, output_dir):
    named_host = socket.gethostname()
    destination = ''.join((output_host, ':', output_dir))
    if named_host == input_host:
        for source in input_paths:
            rsync_copy(source, destination)
            set_move_table(input_host, source, output_host, output_dir)
            shutil.rmtree(source)
        s.close()
    else:
        ssh = dev.login_ssh(output_host)
        for source in input_paths:
            rsync_copy_command = '''rsync -ac {source} {destination}'''.format(source=source, destination=destination)
            rsync_del_command = '''rm -r {source}'''.format(source=source)
            ssh.exec_command(rsync_copy_command)
            set_move_table(input_host, source, output_host, output_dir)
            ssh.exec_command(rsync_del_command)
        ssh.close()
        s.close()

    print('Completed transfer')
    return None

if __name__ == '__main__':
    named_host = socket.gethostname()
    input_host = raw_input('Source directory host: ')
    if named_host == input_host:
        input_paths = glob.glob(raw_input('Source directory path: '))
    else:
        ssh = dev.login_ssh(input_host)
        input_paths = raw_input('Source directory path: ')
        stdin, path_out, stderr = ssh.exec_command('ls -d {input_paths}'.format(input_paths=input_paths))
        input_paths = path_out.read().split('\n')[:-1]
        ssh.close()
        
    input_paths.sort()
    output_host = raw_input('Destination directory host: ')
    output_dir = raw_input('Destination directory: ')
    nulls = null_check(input_host, input_paths)
    if not nulls:
        #if any file not in db -- don't move anything
        print('File(s) not in database')
        sys.exit()
    move_files(input_host, input_paths, output_host, output_dir)
