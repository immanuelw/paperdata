'''
paper.data.file_data

author | Immanuel Washington

Functions
---------
byte_size | gets size of directory or file
human_size | converts amount of bytes into human friendly MB
calc_size | gets human readable size of any directory or file
get_md5sum | generates md5 checksum of file
calc_md5sum | gets md5 checksum of any file
file_names | breaks path into base directory, filename, and filetype
source_info | gets source host and paths string from command line or user input
parse_sources | pulls list of paths from source paths str
'''
from __future__ import print_function
import os
import sys
import glob
import hashlib
import socket
import paper as ppdata

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

def calc_size(host, path):
    '''
    calculates size of directory or file on any host
    logins into host if necessary

    Parameters
    ----------
    host | str: host of file
    path | str: path of directory or file

    Returns
    -------
    float: size of directory or file in MB

    >>> calc_size('folio', '/home/immwa/test_data/zen.2456617.17386.xx.uvcRRE')
    205.2
    '''
    if host == socket.gethostname():
        size_bytes = byte_size(path)
    else:
        with ppdata.ssh_scope(host) as ssh:
            with ssh.open_sftp() as sftp:
                size_bytes = sftp.stat(path).st_size

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

    >>> get_md5sum('/home/immwa/test_data/zen.2456617.17386.xx.uvcRRE')
    '7d5ac942dd37c4ddfb99728359e42331'
    '''
    vis_file = os.path.join(path, 'visdata')
    uv_file = path if os.path.isdir(path) else vis_file if os.path.isfile(visfile)
    hasher = hashlib.md5()

    with open(uv_file, 'rb') as hash_file:
        BLOCKSIZE = 65536
        buf = hash_file.read(BLOCKSIZE)
        while len(buf) > 0:
            hasher.update(buf)
            buf = hash_file.read(BLOCKSIZE)

    return hasher.hexdigest()

def calc_md5sum(host, path):
    '''
    calculates md5 checksum of directory or file on any host
    logins into host if necessary

    Parameters
    ----------
    host | str: host of file
    path | str: path of directory or file

    Returns
    -------
    str: md5 checksum

    >>> calc_md5sum('folio', '/home/immwa/test_data/zen.2456617.17386.xx.uvcRRE')
    '7d5ac942dd37c4ddfb99728359e42331'
    '''
    if host == socket.gethostname():
        md5 = get_md5sum(path)
    else:
        with ppdata.ssh_scope(host) as ssh:
            try:
                with ssh.open_sftp() as sftp:
                    with sftp.file(path, mode='r') as remote_path:
                        md5 = remote_path.check('md5', block_size=65536)
            except(IOError):
                _, md5_out, _ = ssh.exec_command('md5sum {vis_path}'.format(vis_path=os.path.join(path, 'visdata')))
                md5 = md5_out.read().split()[0]

    return md5

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

    >>> file_names('/home/immwa/test_data/zen.2456617.17386.xx.uvcRRE')
    ('/home/immwa/test_data', 'zen.2456617.17386.xx.uvcRRE', 'uvcRRE')
    '''
    base_path, filename = os.path.split(path)
    filetype = filename.split('.')[-1]

    return base_path, filename, filetype

def parse_sources(source_host, source_paths_str):
    '''
    parses source path string and gets list of sources

    Parameters
    ----------
    source_host | str: host of files
    source_paths_str | str: string to indicate paths of uv* files

    Returns
    -------
    list[str]: list of source paths
    '''
    if source_host == socket.gethostname():
        source_paths = glob.glob(source_paths_str)
    else:
        with ppdata.ssh_scope(source_host) as ssh:
            _, path_out, _ = ssh.exec_command('ls -d {source_paths_str}'.format(source_paths_str=source_paths_str))
            source_paths = path_out.read().splitlines()[:-1]

    return source_paths

def source_info(ask=True):
    '''
    gets source information including host and paths from command line
    if None, asks user to supply

    Parameters
    ----------
    ask | bool: get host and paths string from user or not

    Returns
    -------
    tuple:
        str: source host
        list[str]: list of source paths
    OR
    tuple:
        None for every field if no corresponding observation found
    '''
    if ask:
        source_host = raw_input('Source directory host: ')
        source_paths_str = raw_input('Source directory path: ')
    else:
        try:
            source_host, source_paths_str = sys.argv[1].split(':')
        except:
            print('Wrong format for host and paths')
            return (None,) * 2

    source_paths = parse_sources(source_host, source_paths_str)

    return source_host, source_paths

if __name__ == '__main__':
    print('Not a script file, just a module')
