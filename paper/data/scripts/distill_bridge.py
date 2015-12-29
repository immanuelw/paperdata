'''
paper.data.scripts.distill_bridge

pulls info about files and their related observations from the paperdistiller database, then adds to the paperdata database

author | Immanuel Washington

Functions
---------
get_observations | pulls observation objects from paperdistiller to be added to paperdata
add_data | adds info from paperdistiller to paperdata
paperbridge | moves files that have completed compression to preferred directory
'''
import os
import copy
import time
import socket
import uuid
from collections import Counter
import aipy as A
from paper.data import dbi as pdbi, uv_data, file_data
from paper.distiller import dbi as ddbi
import paper.memory as memory
import add_files, move_files

def get_observations(s):
    '''
    gets all observation objects in distiller database if completed for that julian day

    Parameters
    ----------
    s | object: session object

    Returns
    -------
    list[object]: list of observation objects
    '''
    table = ddbi.Observation
    OBSs_all = s.query(table).all()
    OBSs_complete = s.query(table).filter(table.status == 'COMPLETE').all()

    julian_obs = {OBS: int(OBS.julian_date) for OBS in OBSs_complete}
    julian_days = tuple(jday for jday in julian_obs.values())
    count_jdays = Counter(julian_days)

    all_days = tuple(int(OBS.julian_date) for OBS in OBSs_all)
    count_all_days = Counter(all_days)

    complete_jdays = tuple(day for day, amount in count_jdays.items() if amount == count_all_days[day])
    raw_OBSs = tuple(OBS for OBS, jday in julian_obs.items() if jday in complete_jdays)

    return raw_OBSs

def add_data(dbi, data_dbi):
    '''
    transfer data from paperdistiller database to create data for paperdata tables

    Parameters
    ----------
    dbi | object: distiller database interface object
    data_dbi | object: data database interface object

    Returns
    -------
    dict: movable paths for each filetype
    '''
    with dbi.session_scope() as s:
        raw_OBSs = get_observations(s)

        with data_dbi.session_scope() as sess:
            #need to keep dict of list of files to move of each type
            movable_paths = {'uv':[], 'uvcRRE':[], 'npz':[]}

            named_host = socket.gethostname()
            for OBS in raw_OBSs:
                table = ddbi.File
                FILE = s.query(table).filter(table.obsnum == OBS.obsnum).one()

                host = FILE.host
                path = FILE.filename
                base_path, filename, filetype = file_data.file_names(path)
                source = ':'.join((host, path))

                julian_date = OBS.julian_date
                polarization = 'all' if julian_date < 2456400 else OBS.pol

                if host == named_host:
                    try:
                        uv = A.miriad.UV(path)
                        time_start, time_end, delta_time, _  = uv_data.calc_times(uv)
                    except:
                        continue
                else:
                    time_start, time_end, delta_time, _, _, _, _ = uv_data.calc_uv_data(host, path)

                era, julian_day, lst = uv_data.date_info(julian_date)

                md5 = FILE.md5sum
                if md5 is None:
                    md5 = file_data.calc_md5sum(host, path)

                timestamp = int(time.time())

                obs_info = {'obsnum': OBS.obsnum,
                            'julian_date': julian_date,
                            'polarization': polarization,
                            'julian_day': julian_day,
                            'lst': lst,
                            'era': era,
                            'era_type': None,
                            'length': OBS.length,
                            'time_start': time_start,
                            'time_end': time_end,
                            'delta_time': delta_time,
                            'prev_obs': None,
                            'next_obs': None,
                            'is_edge': None,
                            'timestamp': timestamp}

                raw_info = {'host': host,
                            'base_path': base_path,
                            'filename': filename,
                            'filetype': filetype,
                            'source': source,
                            'obsnum': OBS.obsnum,
                            'filesize': file_data.calc_size(host, path),
                            'md5sum': md5,
                            'tape_index': None,
                            'init_host': host,
                            'is_tapeable': True,
                            'is_deletable': False,
                            'timestamp': timestamp}

                log_info = {'action': 'add by bridge',
                            'table': None,
                            'identifier': source,
                            'log_id': str(uuid.uuid4()),
                            'timestamp': timestamp}

                data_dbi.add_entry_dict(sess, 'Observation', obs_info)
                data_dbi.add_entry_dict(sess, 'File', raw_info)
                data_dbi.add_entry_dict(sess, 'Log', log_info)
                movable_paths[filetype].append(path)

                compr_filename = ''.join((filename, 'cRRE'))
                compr_path = os.path.join(base_path, compr_filename)
                if os.path.isdir(compr_path):
                    compr_filetype = 'uvcRRE'
                    compr_info = copy.deepcopy(raw_info)
                    compr_entry = {'filename': compr_filename,
                                    'filetype': compr_filetype,
                                    'filesize': file_data.calc_size(host, base_path, compr_filename),
                                    'md5sum': file_data.calc_md5sum(host, base_path, compr_filename),
                                    'is_tapeable': False}
                    compr_info.update(compr_entry)
                    data_dbi.add_entry_dict(sess, 'File', compr_info)
                    movable_paths[compr_filetype].append(compr_path)

                npz_filename = ''.join((filename, 'cRE.npz'))
                npz_path = os.path.join(base_path, npz_filename)
                if os.path.isfile(npz_path):
                    npz_filetype = 'npz'
                    npz_info = copy.deepcopy(raw_info)
                    npz_entry = {'filename': npz_filename,
                                'filetype': npz_filetype,
                                'filesize': file_data.calc_size(host, base_path, npz_filename),
                                'md5sum': file_data.calc_md5sum(host, base_path, npz_filename),
                                'is_tapeable': False}
                    npz_info.update(npz_entry)
                    data_dbi.add_entry_dict(sess, 'File', npz_info)
                    movable_paths[npz_filetype].append(npz_path)

    return movable_paths

def paperbridge(dbi, data_dbi, auto=False):
    '''
    bridges paperdistiller and paperdata
    moves files and pulls relevant data to add to paperdata from paperdistiller

    Parameters
    ----------
    dbi | object: distiller database interface object
    data_dbi | object: data database interface object
    auto | bool: track whether to wait -- defaults to False
    '''
    #Calculate amount of memory needed to move a day ~1.1TB
    required_memory = 1112661213184
    memory_path = '/data4/paper/raw_to_tape/'

    if memory.enough_memory(required_memory, memory_path):
        source_host = raw_input('Source directory host: ')
        #Add observations and paths from paperdistiller
        movable_paths = add_data(dbi, data_dbi)
        filetypes = movable_paths.keys()
        host_dirs = {filetype: {'host': None, 'dir': None} for filetype in filetypes}
        for filetype in filetypes:
            host_dirs[filetype]['host'] = raw_input('{filetype} destination directory host: '.format(filetype=filetype))
            host_dirs[filetype]['dir'] = raw_input('{filetype} destination directory: '.format(filetype=filetype))

        for filetype, source_paths in movable_paths:
            move_files.move_files(dbi, source_host, source_paths, host_dirs[filetype]['host'], host_dirs[filetype]['dir'])

    else:
        table = 'paperdistiller'
        memory.email_memory(table)
        if auto:
            time.sleep(14400)

if __name__ == '__main__':
    dbi = ddbi.DataBaseInterface()
    data_dbi = pdbi.DataBaseInterface()
    paperbridge(dbi, data_dbi)
    add_files.update_obsnums(data_dbi)
    add_files.connect_observations(data_dbi)
