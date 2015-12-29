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
import dbi as dev

### Script to move files and update paper database
### Move files and update db using dbi

### Author: Immanuel Washington
### Date: 5-06-15

def delete_check(input_host):
    dbi = dev.DataBaseInterface()
    s = dbi.Session()
    table = getattr(dev, 'File')
    FILEs = s.query(table).filter(getattr(table, 'delete_file') == True).filter(getattr(table, 'tape_index') != None)\
                            .filter(getattr(table, 'host') == input_host).all()
    s.close()
    #all files on same host
    full_paths = tuple(os.path.join(FILE.path, FILE.filename) for FILE in FILEs)
    return full_paths

def set_delete_table(input_host, source, output_host, output_dir):
    #change in database
    dbi = dev.DataBaseInterface()
    action = 'delete'
    table = 'file'
    full_path = ''.join((input_host, ':', source))
    timestamp = int(time.time())
    FILE = dbi.get_entry('file', full_path)
    dbi.set_entry(FILE, 'host', output_host)
    dbi.set_entry(FILE, 'path', output_dir)
    dbi.set_entry(FILE, 'delete_file', False)
    dbi.set_entry(FILE, 'timestamp', timestamp)
    identifier = full_path
    log_data = {'action':action,
                'table':table,
                'identifier':identifier,
                'timestamp':timestamp}
    dbi.add_to_table('log', log_data)
    return None

def rsync_copy(source, destination):
    subprocess.check_output(['rsync', '-ac', source, destination])
    return None

def delete_files(input_host, input_paths, output_host, output_dir):
    named_host = socket.gethostname()
    destination = ''.join((output_host, ':', output_dir))
    if named_host == input_host:
        for source in input_paths:
            rsync_copy(source, destination)
            set_delete_table(input_host, source, output_host, output_dir)
            shutil.rmtree(source)
    else:
        ssh = dev.login_ssh(host)
        for source in input_paths:
            rsync_copy_command = '''rsync -ac {source} {destination}'''.format(source=source, destination=destination)
            rsync_del_command = '''rm -r {source}'''.format(source=source)
            ssh.exec_command(rsync_copy_command)
            set_delete_table(input_host, source, output_host, output_dir)
            ssh.exec_command(rsync_del_command)
        ssh.close()

    print('Completed transfer')
    return None

if __name__ == '__main__':
    input_host = raw_input('Source directory host: ')
    output_host = raw_input('Destination directory host: ')
    output_dir = raw_input('Destination directory: ')
    input_paths = delete_check(input_host)
    delete_files(input_host, input_paths, output_host, output_dir)
