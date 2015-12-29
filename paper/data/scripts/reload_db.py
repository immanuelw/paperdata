'''
paper.data.scripts.reload_db

crawls host for all files that should/can be loaded into the paperdata database, then adds them

author | Immanuel Washington

Functions
---------
find_paths | finds all possible uv* files to be added
'''
import os
import sys
import socket
import paper as ppdata
import add_files

def find_paths(input_host):
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
        for root, dirs, files in os.walk('/'):
            for direc in dirs:
                if direc.endswith('uv') or direc.endswith('uvcRRE'):
                    uv_paths.append(os.path.join(root, direc))
            for file_path in files:
                if file_path.endswith('npz'):
                    npz_paths.append(os.path.join(root, file_path))
    else:
        with ppdata.login_ssh(input_host) as ssh:
            find = '''find / -name '*.uv' -o -name '*.uvcRRE' -o -name '*.npz' 2>/dev/null'''
            _, all_paths, _ = ssh.exec_command(find)
        for path in all_paths.splitlines():
            if direc.endswith('uv') or direc.endswith('uvcRRE'):
                 uv_paths.append(path)
            elif file_path.endswith('npz'):
                 npz_paths.append(path)

    return sorted(uv_paths), sorted(npz_paths)

if __name__ == '__main__':
    if len(sys.argv) == 2:
        input_host = sys.argv[1]
    else:
        input_host = raw_input('Source directory host: ')

    for paths in find_paths(input_host):
        add_files.add_files(input_host, paths)
