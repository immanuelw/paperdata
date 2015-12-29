'''
paper.data.uv_data

author | Immanuel Washington

Functions
---------
five_round | rounds value to five decimal places
jdpol_to_obsnum | generates unique observation number
date_info | pulls information about observation from julian date
is_edge | checks if observation is on edge of observing period
calc_times | pulls time information from uv file object
calc_npz_data | pulls relevant observation information from npz file
calc_uv_data | pulls all relevant information from any uv* file
'''
from __future__ import print_function
import os
import sys
import socket
import paper as ppdata
from paper.data import dbi as pdbi
import paper.convert as convert
import aipy as A

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
    polnum = pdbi.str_to_pol[pol] + 10
    assert(obsint < 2 ** 31)

    return int(obsint + polnum * (2 ** 32))

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
    era = 32 if julian_date < 2456100 else 64 if julian_date < 2456400 else 128
    julian_day = int(julian_date)

    gmst = convert.juliandate_to_gmst(julian_date)
    lst = convert.gmst_to_lst(gmst, longitude=25)

    return era, julian_day, round(lst, 1)

def is_edge(prev_obs, next_obs):
    '''
    checks if observation is on the edge of each day's observation cycle

    Parameters
    ----------
    prev_obs (database object): previous observation
    next_obs (database object): next observation

    Returns
    -------
    bool: on the edge of a julian day or None

    >>> is_edge(None, None)
    ...
    >>> is_edge(None, object)
    True
    >>> is_edge(object, object)
    False
    '''
    if (prev_obs, next_obs) == (None, None):
        is_edge = None
    else:
        is_edge = (None in (prev_obs, next_obs))

    return is_edge

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

    >>> calc_times(A.miriad.UV('/home/immwa/test_data/zen.2456617.17386.xx.uvcRRE'))
    (2456617.17386, 2456617.18032, 0.0005, 0.00696)
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

    delta_time = -(time_start - time_end) / (n_times - 1)

    length = five_round(n_times * delta_time)
    time_start = five_round(time_start)
    time_end = five_round(time_end)
    delta_time = five_round(delta_time)

    return time_start, time_end, delta_time, length

def calc_npz_data(dbi, filename):
    '''
    takes in npz files and pulls data about observation

    Parameters
    ----------
    dbi | object: database interface object
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

    >>> calc_npz_data(pdbi.DataBaseInterface(), '/home/immwa/test_data/zen.2455906.53332.uvcRE.npz')
    (Decimal('2455906.53332'), Decimal('2455906.54015'), Decimal('0.00012'), 2455906.53332, 'all', Decimal('0.00696'), 17185743685L)
    '''
    filetype = filename.split('.')[-1]
    if filetype not in ('npz',):
        return (None,) * 7
    
    julian_date = ppdata.file_to_jd(filename)

    with dbi.session_scope() as s:
        polarization = filename.split('.')[3] if len(filename.split('.')) == 6 else 'all'
        table = pdbi.Observation
        OBS = s.query(table).filter(table.julian_date == julian_date).filter(table.polarization == polarization).one()

        return OBS.time_start, OBS.time_end, OBS.delta_time, julian_date, polarization, OBS.length, OBS.obsnum

def calc_uv_data(host, path):
    '''
    takes in uv* files and pulls data about observation

    Parameters
    ----------
    host | str: host of system
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

    >>> calc_uv_data('folio', '/home/immwa/test_data/zen.2456617.17386.xx.uvcRRE')
    (2456617.17386, 2456617.18032, 0.0005, 2456617.18069, 'xx', 0.00696, 21480813086)
    '''
    named_host = socket.gethostname()
    if named_host == host:
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
            polarization = pdbi.pol_to_str[uv['pol']] if uv['npol'] == 4 else 'all' if uv['npol'] == 4

            obsnum = jdpol_to_obsnum(julian_date, polarization, length) if length > 0 else None

    else:
        uv_data_script = os.path.expanduser('~/paperdata/paper/data/uv_data.py')
        moved_script = './uv_data.py'
        uv_comm = 'python {moved_script} {host} {path}'.format(moved_script=moved_script, host=host, path=path)
        with ppdata.ssh_scope(host) as ssh:
            with ssh.open_sftp() as sftp:
                try:
                    filestat = sftp.stat(uv_data_script)
                except(IOError):
                    try:
                        filestat = sftp.stat(moved_script)
                    except(IOError):
                        sftp.put(uv_data_script, moved_script)

            _, uv_dat, _ = ssh.exec_command(uv_comm)
            time_start, time_end, delta_time, julian_date, polarization, length, obsnum = [five_round(info) if key in (0, 1, 2, 3, 5)
                                                                                            else int(info) if key in (6,)
                                                                                            else info
                                                                                            for key, info in enumerate(uv_dat.read().split(','))]

    return time_start, time_end, delta_time, julian_date, polarization, length, obsnum

if __name__ == '__main__':
    source_host = sys.argv[1]
    source_path = sys.argv[2]

    uv_data = calc_uv_data(source_host, source_path)
    if uv_data is None:
        sys.exit()
    output_string = ','.join(uv_data)
    print(output_string)
