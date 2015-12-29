'''
paper.data.scripts.add_files

pulls info about files and their related observations, then adds to the paperdata database

author | Immanuel Washington

Functions
---------
calc_obs_info | pulls observation and file data from files
dupe_check | checks database for duplicate files
add_files_to_db | pulls file and observation data and adds to database
add_files | parses list of files and adds data to database
'''
from __future__ import print_function
import os
import time
import uuid
from paper.data import dbi as pdbi, uv_data, file_data
import refresh_db

def calc_obs_info(dbi, host, path):
    '''
    generates all relevant data from uv* file

    Parameters
    ----------
    dbi | object: database interface object
    host | str: host of system
    path | str: path of uv* file

    Returns
    -------
    tuple:
        dict: observation values
        dict: file values
        dict: log values
    '''
    base_path, filename, filetype = file_data.file_names(path)
    source = ':'.join((host, path))

    if filetype in ('uv', 'uvcRRE'):
        time_start, time_end, delta_time, julian_date, polarization, length, obsnum = uv_data.calc_uv_data(host, path)
    elif filetype in ('npz',):
        time_start, time_end, delta_time, julian_date, polarization, length, obsnum = uv_data.calc_npz_data(dbi, filename)

    era, julian_day, lst = uv_data.date_info(julian_date)

    timestamp = int(time.time())

    obs_info = {'obsnum': obsnum,
                'julian_date': julian_date,
                'polarization': polarization,
                'julian_day': julian_day,
                'lst': lst,
                'era': era,
                'era_type': None,
                'length': length,
                'time_start': time_start,
                'time_end': time_end,
                'delta_time': delta_time,
                'prev_obs': None, 
                'next_obs': None,
                'is_edge': None,
                'timestamp': timestamp}

    file_info = {'host': host,
                'base_path': base_path,
                'filename': filename,
                'filetype': filetype,
                'source': source,
                'obsnum': obsnum,
                'filesize': file_data.calc_size(host, path),
                'md5sum': file_data.calc_md5sum(host, path),
                'tape_index': None,
                'init_host': host,
                'is_tapeable': False,
                'is_deletable': False,
                'timestamp': timestamp}

    log_info = {'action': 'add by scan',
                'table': None,
                'identifier': source,
                'log_id': str(uuid.uuid4()),
                'timestamp': timestamp}

    return obs_info, file_info, log_info

def dupe_check(dbi, source_host, source_paths):
    '''
    checks for duplicate paths and removes to not waste time if possible
    checks for paths only on same host

    Parameters
    ----------
    dbi | object: database interface object
    source_host | str: host of uv* files
    source_paths | list[str]: paths of uv* files

    Returns
    -------
    list[str]: paths that are not already in database
    '''
    with dbi.session_scope() as s:
        table = pdbi.File
        FILEs = s.query(table).filter(table.host == source_host).all()
        paths = tuple(os.path.join(FILE.base_path, FILE.filename) for FILE in FILEs)

    unique_paths = tuple(source_path for source_path in source_paths if source_path not in paths)
        
    return unique_paths

def add_files_to_db(dbi, source_host, source_paths):
    '''
    adds files to the database

    Parameters
    ----------
    dbi | object: database interface object
    source_host | str: host of files
    source_paths | list[str]: paths of uv* files
    '''
    with dbi.session_scope() as s:
        for source_path in source_paths:
            obs_info, file_info, log_info = calc_obs_info(source_host, source_path)
            try:
                dbi.add_entry_dict(s, 'Observation', obs_info)
            except:
                print('Failed to load in obs ', source_path)
            try:
                dbi.add_entry_dict(s, 'File', file_info)
            except:
                print('Failed to load in file ', source_path)
            try:
                dbi.add_entry_dict(s, 'Log', log_info)
            except:
                print('Failed to load in log ', source_path)

def add_files(dbi, source_host, source_paths):
    '''
    generates list of input files, check for duplicates, add information to database

    Parameters
    ----------
    dbi | object: database interface object
    source_host | str: host of files
    source_paths | list[str]: list of paths of uv* files
    '''
    source_paths = sorted(dupe_check(dbi, source_host, source_paths))

    npz_paths = [npz_path for npz_path in source_paths if npz_path.endswith('.npz')]
    uv_paths = [uv_path for uv_path in source_paths if not uv_path.endswith('.npz')]
    add_files_to_db(dbi, source_host, uv_paths)
    add_files_to_db(dbi, source_host, npz_paths)
    #refresh_db.refresh_db(dbi)

if __name__ == '__main__':
    source_host, source_paths = file_data.source_info()
    dbi = pdbi.DataBaseInterface()
    add_files(dbi, source_host, source_paths)
