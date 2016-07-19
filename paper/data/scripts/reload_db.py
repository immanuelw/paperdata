'''
paper.data.scripts.reload_db

crawls host for all files that should/can be loaded into the paperdata database, then adds them

author | Immanuel Washington

Functions
---------
find_paths | finds all possible uv* files to be added
'''
from __future__ import print_function
import os
import sys
import argparse
import socket
import paper as ppdata
from paper.data import dbi as pdbi
import add_files

def find_paths(input_host, base_dir='/'):
    '''
    finds all uv* files on a host

    Parameters
    ----------
    input_host | str: system host of files

    Returns
    -------
    tuple:
        list: uv* files
        list: .npz files
    '''
    uv_paths = []
    npz_paths = []
    if input_host == socket.gethostname():
        for root, dirs, files in os.walk(base_dir):
            for direc in dirs:
                if direc.endswith('.uv') or direc.endswith('.uvcRRE'):
                    print(direc)
                    uv_paths.append(os.path.join(root, direc))
            for file_path in files:
                if file_path.endswith('.npz'):
                    npz_paths.append(os.path.join(root, file_path))
    else:
        with ppdata.login_ssh(input_host) as ssh:
            find = '''find / -name '*.uv' -o -name '*.uvcRRE' -o -name '*.npz' 2>/dev/null'''
            _, all_paths, _ = ssh.exec_command(find)
        for path in all_paths.splitlines():
            if direc.endswith('.uv') or direc.endswith('.uvcRRE'):
                 uv_paths.append(path)
            elif file_path.endswith('.npz'):
                 npz_paths.append(path)

    return sorted(uv_paths), sorted(npz_paths)

def write_paths_to_file(file_path, all_paths):
    '''
    writes all paths found to file

    Parameters
    ----------
    file_path | str: path of file to write to
    all_paths | tuple: tuple of lists of paths to write

    Returns
    -------
    '''
    uv_file = os.path.join(file_path, 'uv_files.txt')
    npz_file = os.path.join(file_path, 'npz_files.txt')
    files = (uv_file, npz_file)
    for filename, paths in zip(files, all_paths):
        with open(filename, 'w') as f:
            for path in paths:
                f.write(''.join((path, '\n')))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Add files to the database')
    parser.add_argument('--source_host', type=str, help='source host')
    parser.add_argument('--source_dir', type=str, help='source directory')

    args = parser.parse_args()

    source_host = args.source_host
    source_dir = glob.glob(args.source_dir)

    all_paths = find_paths(input_host, base_dir=source_dir)
    #all_paths = find_paths(input_host, base_dir='/data3/paper')
    file_path = os.path.expanduser('~/paperdata/paper/data/src/')
    write_paths_to_file(file_path, all_paths)

    dbi = pdbi.DataBaseInterface()
    for paths in all_paths:
        add_files.add_files(s, input_host, paths)
