#!/usr/bin/python
# -*- coding: utf-8 -*-
# Load data into MySQL table 

import sys
import os
import time
import socket
from collections import Counter
import aipy as A
import ddr_compress.dbi as ddbi
from sqlalchemy import func
import dbi as dev
import add_files
import uv_data
import move_files

### Script to load infromation quickly from paperdistiller database into paper
### Queries paperdistiller for relevant information, loads paper with complete info

### Author: Immanuel Washington
### Date: 8-20-14
def calc_time_data(host):
    ssh = dev.login_ssh(host)
    time_data_script = os.path.expanduser('~/paper/dbi/scripts/time_data.py')
    sftp = ssh.open_sftp()
    moved_script = './time_data.py'
    try:
        filestat = sftp.stat(time_data_script)
    except(IOError):
        try:
            filestat = sftp.stat(moved_script)
        except(IOError):
            sftp.put(time_data_script, moved_script)
    sftp.close()
    stdin, time_data, stderr = ssh.exec_command('python {moved_script} {host} {full_path}'.format(moved_script=moved_script, host=host, full_path=full_path))
    time_start, time_end, delta_time = [float(info) for info in time_data.read().split(',')]
    ssh.close()

    return time_start, time_end, delta_time

def add_data():
    dbi = ddbi.DataBaseInterface()
    s = dbi.Session()
    #do stuff
    table = getattr(ddbi, 'Observation')
    OBSs_all = s.query(table).all()
    OBSs_complete = s.query(table).filter(getattr(table, 'status') == 'COMPLETE').all()
    s.close()

    julian_obs = {OBS: int(str(getattr(OBS, 'julian_date'))[3:7]) for OBS in OBSs_complete}
    julian_days = tuple(jday for jday in julian_obs.values())
    #dict of julian day as key, amount as value
    count_jdays = Counter(julian_days)

    all_days = tuple(int(str(getattr(OBS, 'julian_date'))[3:7]) for OBS in OBSs_all)
    count_all_days = Counter(all_days)

    #tuple list of all complete days
    complete_jdays = tuple(day for day, amount in count_jdays.items() if amount == count_all_days[day])
    raw_OBSs = tuple(OBS for OBS, jday in julian_obs.items() if jday in complete_jdays)

    #check for duplicates
    dev_dbi = dev.DataBaseInterface()

    #need to keep dict of list of files to move of each type -- (host, path, filename, filetype)
    movable_paths = {'uv':[], 'uvcRRE':[], 'npz':[]}

    named_host = socket.gethostname()
    for OBS in raw_OBSs:
        table = getattr(ddbi, 'File')
        FILE = s.query(table).filter(getattr(table, 'obsnum') == getattr(OBS, 'obsnum')).one()

        host = getattr(FILE, 'host')
        full_path = getattr(FILE, 'filename')
        path, filename, filetype = add_files.file_names(full_path)

        obsnum = getattr(OBS, 'obsnum')
        julian_date = getattr(OBS, 'julian_date')
        if julian_date < 2456400:
            polarization = 'all'
        else:
            polarization = getattr(OBS, 'pol')
        length = getattr(OBS, 'length')
    
        if named_host == host:
            try:
                uv = A.miriad.UV(full_path)
            except:
                continue

            time_start, time_end, delta_time, _  = uv_data.calc_times(uv)

        else:
            time_start, time_end, delta_time, _ = add_files.get_uv_data(host, full_path, mode='time')
        
        era, julian_day = add_files.julian_era(julian_date)

        #indicates type of file in era
        era_type = None

        prev_obs, next_obs, edge = add_files.obs_edge(obsnum, sess=sp)

        filesize = add_files.calc_size(host, path, filename)
        md5 = getattr(FILE, 'md5sum')
        if md5 is None:
            md5 = add_files.calc_md5sum(host, path, filename)
        tape_index = None

        source_host = host
        write_to_tape = True
        delete_file = False

        timestamp = int(time.time())

        obs_data = {'obsnum':obsnum,
                    'julian_date':julian_date,
                    'polarization':polarization,
                    'julian_day':julian_day,
                    'era':era,
                    'era_type':era_type,
                    'length':length,
                    'time_start':time_start,
                    'time_end':time_end,
                    'delta_time':delta_time,
                    'prev_obs':prev_obs, 
                    'next_obs':next_obs,
                    'edge':edge,
                    'timestamp':timestamp}
        raw_data = {'host':host,
                    'path':path,
                    'filename':filename,
                    'filetype':filetype,
                    'full_path':full_path,
                    'obsnum':obsnum,
                    'filesize':filesize,
                    'md5sum':md5,
                    'tape_index':tape_index,
                    'source_host':source_host,
                    'write_to_tape':write_to_tape,
                    'delete_file':delete_file,
                    'timestamp':timestamp}
        action = 'add by bridge'
        table = None
        identifier = full_path
        log_data = {'action':action,
                    'table':table,
                    'identifier':identifier,
                    'timestamp':timestamp}
        dev_dbi.add_to_table('observation', obs_data)
        dev_dbi.add_to_table('file', raw_data)
        dev_dbi.add_to_table('log', log_data)
        movable_paths[filetype].append(os.path.join(path, filename))

        compr_filename = ''.join((filename, 'cRRE'))
        compr_filetype = 'uvcRRE'
        compr_filesize = add_files.calc_size(host, path, compr_filename)
        compr_md5 = add_files.calc_md5sum(host, path, compr_filename)
        compr_write_to_tape = False
        if os.path.isdir(os.path.join(path, compr_filename)):
            compr_data = raw_data
            compr_data['filename'] = compr_filename
            compr_data['filetype'] = compr_filetype
            compr_data['filesize'] = compr_filesize
            compr_data['md5sum'] = compr_md5
            compr_data['write_to_tape'] = compr_write_to_tape
            dev_dbi.add_to_table('file', compr_data)
            movable_paths[compr_filetype].append(os.path.join(path, compr_filename))

        npz_filename = ''.join((filename, 'cRE.npz'))
        npz_filetype = 'npz'
        npz_filesize = add_files.calc_size(host, path, npz_filename)
        npz_md5 = add_files.calc_md5sum(host, path, npz_filename)
        npz_write_to_tape = False
        if os.path.isfile(os.path.join(path, npz_filename)):
            npz_data = raw_data
            npz_data['filename'] = npz_filename
            npz_data['filetype'] = npz_filetype
            npz_data['filesize'] = npz_filesize
            npz_data['md5sum'] = npz_md5
            npz_data['write_to_tape'] = npz_write_to_tape
            dev_dbi.add_to_table('file', npz_data)
            movable_paths[npz_filetype].append(os.path.join(path, npz_filename))

    return movable_paths

def bridge_move(input_host, movable_paths, raw_host, raw_dir, compr_host, compr_dir, npz_host, npz_dir):
    raw_paths = movable_paths['uv']
    compr_paths = movable_paths['uvcRRE']
    npz_paths = movable_paths['npz']

    move_files.move_files(input_host, raw_paths, raw_host, raw_dir)
    move_files.move_files(input_host, compr_paths, compr_host, compr_dir)
    move_files.move_files(input_host, npz_paths, npz_host, npz_dir)

    return None

def paperbridge():
    #Calculate amount of space needed to move a day ~1.1TB
    required_space = 1112661213184
    space_path = '/data4/paper/raw_to_tape/'

    if move_files.enough_space(required_space, space_path):
        input_host = raw_input('Source directory host: ')
        #Add observations and paths from paperdistiller
        movable_paths = add_data()
        raw_host = raw_input('Raw destination directory host: ')
        raw_dir = raw_input('Raw destination directory: ')
        compr_host = raw_input('Compressed destination directory host: ')
        compr_dir = raw_input('Compressed destination directory: ')
        npz_host = raw_input('Npz destination directory host: ')
        npz_dir = raw_input('Npz destination directory: ')

        bridge_move(input_host, movable_paths, raw_host, raw_dir, compr_host, compr_dir, npz_host, npz_dir)

    else:
        table = 'paperdistiller'
        move_files.email_space(table)
        if auto == 'y':
            time.sleep(14400)

    return None

if __name__ == '__main__':
    auto = 'n'
    paperbridge(auto)
    add_files.update_obsnums()
