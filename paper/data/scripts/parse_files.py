'''
paper.data.scripts.parse_files

pulls info about files and their related observations, then adds to the paperdata database

author | Immanuel Washington

Functions
---------
decimal_default | json fix for decimal types
byte_size | gets size of directory or file
human_size | converts amount of bytes into human friendly MB
calc_size | gets human readable size of any directory or file
get_md5sum | generates md5 checksum of file
file_names | breaks path into base directory, filename, and filetype
calc_obs_info | pulls observation and file data from files
add_files_to_db | pulls file and observation data and adds to database
add_files | parses list of files and adds data to database
'''
from __future__ import print_function
import os
import decimal
import glob
import hashlib
import json
import re
import socket
import time
import uuid

str_to_pol = {  'I' :  1,   # Stokes Paremeters
                'Q' :  2,
                'U' :  3,
                'V' :  4,
                'rr': -1,   # Circular Polarizations
                'll': -2,
                'rl': -3,
                'lr': -4,
                'xx': -5,   # Linear Polarizations
                'yy': -6,
                'xy': -7,
                'yx': -8,
                'all': -6}

pol_to_str = {v: k for k, v in str_to_pol.items()}

def file_to_jd(path):
    '''
    pulls julian date from filename using regex

    Parameters
    ----------
    path | str: path of file

    Returns
    -------
    str: julian date
    '''
    try:
        jd = round(float(re.findall(r'\d+\.\d+', path)[0]), 5)
    except:
        jd = None

    return jd

def file_to_pol(path):
    '''
    pulls polarization from filename using regex

    Parameters
    ----------
    path | str: path of file

    Returns
    -------
    str: polarization
    '''
    pol = re.findall(r'\.(.{2})\.', path)
    polarization = 'all' if len(pol) == 0 else pol[0]

    return polarization

def decimal_default(obj):
    '''
    fixes decimal issue with json module

    Parameters
    ----------
    obj (object)

    Returns
    -------
    object: float version of decimal object
    '''
    if isinstance(obj, decimal.Decimal):
        return float(obj)

def byte_size(path):
    '''
    output byte size of directory or file

    Parameters
    ----------
    path | str: path of directory or file

    Returns
    -------
    int: amount of bytes

    >>> byte_size('/home/immwa/test_data/zen.2456617.17386.xx.uvcRRE')
    215132692
    '''
    total_size = 0
    for base_path, _, filenames in os.walk(path):
        for filename in filenames:
            total_size += os.path.getsize(os.path.join(base_path, filename))

    return total_size

def human_size(num):
    '''
    converts bytes to MB

    Parameters
    ----------
    num | int: amount of bytes

    Returns
    -------
    float(1): amount of MB to 1 decimal place

    >>> human_size(1048576)
    1.0
    '''
    return round(num / 1024. / 1024., 1)

def calc_size(path):
    '''
    calculates size of directory or file

    Parameters
    ----------
    path | str: path of directory or file

    Returns
    -------
    float: size of directory or file in MB
    '''
    size_bytes = byte_size(path)

    return human_size(size_bytes)

def get_md5sum(path):
    '''
    calculate the md5 checksum of a uv file

    Parameters
    ----------
    path | str: path of directory or file

    Returns
    -------
    str: 32-bit hex integer md5 checksum
    '''
    vis_file = os.path.join(path, 'visdata')
    uv_file = path if os.path.isfile(path) else vis_file if os.path.isfile(vis_file) else None

    if uv_file is None:
        return None

    hasher = hashlib.md5()

    with open(uv_file, 'rb') as hash_file:
        BLOCKSIZE = 65536
        buf = hash_file.read(BLOCKSIZE)
        while len(buf) > 0:
            hasher.update(buf)
            buf = hash_file.read(BLOCKSIZE)

    return hasher.hexdigest()

def file_names(path):
    '''
    separates full path of directory or file into parts

    Parameters
    ----------
    path | str: path of directory or file

    Returns
    -------
    tuple:
        str: base path
        str: directory/file name
        str: extension/filetype

    '''
    base_path, filename = os.path.split(path)
    filetype = filename.split('.')[-1]

    return base_path, filename, filetype

def five_round(num):
    '''
    rounds number to five significant figures

    Parameters
    ----------
    num | float: number

    Returns
    -------
    float(5): number

    >>> five_round(2455903.1667113231)
    2455903.16671
    '''
    return round(num, 5)

def jdpol_to_obsnum(jd, pol, djd):
    '''
    calculates unique observation number for observations

    Parameters
    ----------
    jd | float: julian date float
    pol | str: polarization
    length | float: length of obs in fraction of julian date

    Returns
    -------
    int: a unique integer index for observation

    >>> jdpol_to_obsnum(2456600, 'xx', 0.00696)
    21480810617
    '''
    dublinjd = jd - 2415020  #use Dublin Julian Date
    obsint = int(dublinjd / djd)  #divide up by length of obs
    polnum = str_to_pol[pol] + 10
    assert(obsint < 2 ** 31)

    return int(obsint + polnum * (2 ** 32))

def degrees_to_hours(angle):
    '''
    converts degrees to decimal hours

    Parameters
    ----------
    angle | float: angle in degrees

    Returns
    -------
    float: angle in decimal hours

    >>> degrees_to_hours(12)
    0.8
    '''
    return angle / 15.

def juliandate_to_gmst(juliandate):
    '''
    convert a Julian Date to Greenwich Mean Sidereal Time

    Parameters
    ----------
    juliandate | float: Julian Date

    Returns
    -------
    float: decimal hours in GMST

    >>> juliandate_to_gmst(2455903.0)
    17.060789746482214
    '''
    jd0 = int(juliandate - .5) + .5  # Julian Date of previous midnight
    h = (juliandate - jd0) * 24.  # Hours since mightnight
    # Days since J2000 (Julian Date 2451545.)
    d0 = jd0 - 2451545.
    d = juliandate - 2451545.
    t = d / 36525.  # Centuries since J2000

    gmst = (6.697374558 + 0.06570982441908 * d0 + 1.00273790935 * h +
            0.000026 * t * t)

    return gmst % 24.

def gmst_to_lst(hours, longitude):
    '''
    convert Greenwich Mean Sidereal Time to Local Sidereal Time

    Parameters
    ----------
    hours | float: decimal hours in GMST
    longitude | float: location in degrees, E positive

    Returns
    -------
    float: decimal hours in LST

    >>> gmst_to_lst(14, 20)
    15.333333333333334
    '''
    longitude_time = degrees_to_hours(longitude)
    lst = hours + longitude_time
    lst %= 24

    return lst

def date_info(julian_date):
    '''
    indicates julian day and set of data
    calculates local sidereal hours for that julian date

    Parameters
    ----------
    julian_date | float: julian date

    Returns
    -------
    tuple:
        int: era of julian date
        int: julian day
        float(1): lst hours rounded to one decimal place

    >>> date_info(2456604.16671)
    (128, 2456604, 20.8)
    '''
    if julian_date is None:
        return (None, None, None)

    era = 32 if julian_date < 2456100 else 64 if julian_date < 2456400 else 128
    julian_day = int(julian_date)

    gmst = juliandate_to_gmst(julian_date)
    lst = gmst_to_lst(gmst, longitude=25)

    return era, julian_day, round(lst, 1)

def calc_times(uv):
    '''
    takes in uv file and calculates time based information

    Parameters
    ----------
    uv | file object: uv file object

    Returns
    -------
    tuple:
        float(5): time start
        float(5): time end
        float(5): delta time
        float(5): length of uv file observation
    OR
    tuple:
        None for very field
    '''
    time_start = 0
    time_end = 0
    n_times = 0
    c_time = 0

    try:
        for (_, t, (_, _)), _ in uv.all():
            if time_start == 0 or t < time_start:
                time_start = t
            if time_end == 0 or t > time_end:
                time_end = t
            if c_time != t:
                c_time = t
                n_times += 1
    except:
        return (None,) * 4

    if n_times <= 1:
        delta_time = 0
    else:
        delta_time = -(time_start - time_end) / (n_times - 1)

    length = five_round(n_times * delta_time)
    time_start = five_round(time_start)
    time_end = five_round(time_end)
    delta_time = five_round(delta_time)

    return time_start, time_end, delta_time, length

def calc_npz_data(filename):
    '''
    takes in npz files and pulls data about observation

    Parameters
    ----------
    filename | str: filename of npz file

    Returns
    -------
    tuple:
        float(5): time start
        float(5): time end
        float(5): delta time
        float(5): julian date
        str: polarization
        float(5): length
        int: obsnum of uv file observation
    OR
    tuple:
        None for every field if no corresponding observation found
    '''
    filetype = filename.split('.')[-1]
    if filetype not in ('npz',):
        return (None,) * 7
    
    julian_date = file_to_jd(filename)
    polarization = file_to_pol(filename)

    return (None, None, None, julian_date, polarization, None, None)

def calc_uv_data(path):
    '''
    takes in uv* files and pulls data about observation

    Parameters
    ----------
    path | str: path of uv* file

    Returns
    -------
    tuple:
        float(5): time start
        float(5): time end
        float(5): delta time
        float(5): julian date
        str: polarization
        float(5): length
        int: obsnum of uv file observation
    OR
    tuple:
        None for every field if no corresponding observation found
    '''
    filetype = path.split('.')[-1]
    if filetype not in ('uv', 'uvcRRE'):
        return (None,) * 7
    else:
        try:
            uv = A.miriad.UV(path)
        except:
            return (None,) * 7

        time_start, time_end, delta_time, length = calc_times(uv)
        julian_date = five_round(uv['time'])
        polarization = pol_to_str[uv['pol']] if uv['npol'] == 4 else 'all' if uv['npol'] == 1 else None

        obsnum = jdpol_to_obsnum(julian_date, polarization, length) if length > 0 else None

    return time_start, time_end, delta_time, julian_date, polarization, length, obsnum

def calc_obs_info(host, path):
    '''
    generates all relevant data from uv* file

    Parameters
    ----------
    host | str: host of system
    path | str: path of uv* file

    Returns
    -------
    tuple:
        dict: observation values
        dict: file values
        dict: log values
    '''
    base_path, filename, filetype = file_names(path)
    source = ':'.join((host, path))

    if filetype in ('uv', 'uvcRRE'):
        time_start, time_end, delta_time, julian_date, polarization, length, obsnum = calc_uv_data(path)
    elif filetype in ('npz',):
        time_start, time_end, delta_time, julian_date, polarization, length, obsnum = calc_npz_data(filename)

    era, julian_day, lst = date_info(julian_date)

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
                'filesize': calc_size(path),
                'md5sum': get_md5sum(path),
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

    print(obs_info, file_info)

    return obs_info, file_info, log_info

if __name__ == '__main__':
    source_host = socket.gethostname()
    source_paths_str = raw_input('Source directory path: ')
    source_paths = glob.glob(source_paths_str)

    obs_path = os.path.expanduser('~/obs.json')
    file_path = os.path.expanduser('~/file.json')

    with open(obs_path, 'w') as bkup, open(file_path, 'w') as bkup2:
        pass

    with open(obs_path, 'a') as bkup, open(file_path, 'a') as fkup:
        for source_path in source_paths:
            print(source_path)
            obs_info, file_info, log_info = calc_obs_info(source_host, source_path)
            data = [obs_info]
            json.dump(data, bkup, sort_keys=True, indent=1, default=decimal_default)
            data = [file_info]
            json.dump(data, fkup, sort_keys=True, indent=1, default=decimal_default)
