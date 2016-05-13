'''
paper.data.scripts.parse_files

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
import paper as ppdata
from paper.data import dbi as pdbi, uv_data, file_data

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

def dupe_check(dbi, source_host, source_paths, verbose=False):
    '''
    checks for duplicate paths and removes to not waste time if possible
    checks for paths only on same host

    Parameters
    ----------
    dbi | object: database interface object
    source_host | str: host of uv* files
    source_paths | list[str]: paths of uv* files
    verbose | bool: whether paths are printed or not

    Returns
    -------
    list[str]: paths that are not already in database
    '''
    with dbi.session_scope() as s:
        table = pdbi.File
        FILEs = s.query(table).filter(table.host == source_host).all()
        paths = tuple(os.path.join(FILE.base_path, FILE.filename) for FILE in FILEs)

    unique_paths = set(source_paths) - set(paths)
    if verbose:
        print(len(unique_paths), 'unique paths')

    return unique_paths

if __name__ == '__main__':
    source_host, source_paths = file_data.source_info()
    #dbi = pdbi.DataBaseInterface()
    dbi = None
    obs_path = os.path.expanduser('~/obs.json')
    file_path = os.path.expanduser('~/file.json')
    with open(obs_path, 'w') as bkup:
        pass
    with open(file_path, 'w') as bkup:
        pass
    for source_path in source_paths:
        print(source_path)
        obs_info, file_info, log_info = calc_obs_info(dbi, source_host, source_path)
        with open(obs_path, 'a') as bkup:
            data = [obs_info]
            json.dump(data, bkup, sort_keys=True, indent=1, default=ppdata.decimal_default)
        with open(file_path, 'a') as bkup:
            data = [file_info]
            json.dump(data, bkup, sort_keys=True, indent=1, default=ppdata.decimal_default)
