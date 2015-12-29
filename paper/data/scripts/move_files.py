'''
paper.data.scripts.move_files

moves files and updates database with new host and path

author | Immanuel Washington

Functions
---------
exist_check | checks to see if files to be moved all exist in database
set_move_table | updates database with moved file status
move_files | parses list of files then moves them
'''
from __future__ import print_function
import os
import glob
import socket
import shutil
import time
import uuid
import paper as ppdata
from paper.data import dbi as pdbi, file_data

def exist_check(dbi, source_host, source_paths):
    '''
    checks if file(s) is(are) in database

    Parameters
    ----------
    dbi | object: database interface object
    source_host | str: host of files
    source_paths | list[str]: uv* file paths

    Returns
    -------
    bool: are all files in the database
    '''
    with dbi.session_scope() as s:
        table = pdbi.File
        FILEs = s.query(table).filter(table.host == source_host).all()
        paths = tuple(os.path.join(FILE.path, FILE.filename) for FILE in FILEs)

    return all(source_path in paths for source_path in source_paths)

def set_move_table(s, dbi, source_host, source_path, dest_host, dest_path):
    '''
    updates table for moved file

    Parameters
    ----------
    s | object: session object
    dbi | object: database interface object
    source_host | str: user host
    source_path | str: source file path
    dest_host | str: output host
    dest_path | str: output directory
    '''
    source = ':'.join((source_host, source_path))
    timestamp = int(time.time())
    FILE = dbi.get_entry(s, 'File', source)
    move_dict = {'host': dest_host,
                'base_path': dest_path,
                'timestamp': timestamp}
    dbi.set_entry_dict(s, FILE, move_dict)

    log_data = {'action': 'move',
                'table': 'File',
                'identifier': FILE.source,
                'log_id': str(uuid.uuid4()),
                'timestamp': timestamp}
    dbi.add_entry_dict(s, 'Log', log_data)

def move_files(dbi, source_host=None, source_paths=None, dest_host=None, dest_path=None):
    '''
    move files by rsyncing them and checking md5sum through rsync option

    Parameters
    ----------
    dbi | object: database interface object
    source_host | str: file host --defaults to None
    source_paths | list[str]: file paths --defaults to None
    dest_host | str: output host --defaults to None
    dest_path | str: output directory --defaults to None
    '''
    dest_host = raw_input('Destination directory host: ') if dest_host is None else dest_host
    dest_path = raw_input('Destination directory: ') if dest_path is None else dest_path

    if source_host is None or source_paths is None:
        source_host, source_paths = file_data.source_info()

    is_existent = exist_check(source_host, source_paths)
    if not is_existent:
        print('File(s) not in database')
        return

    destination = ':'.join((dest_host, dest_path))
    with dbi.session_scope() as s:
        if source_host == socket.gethostname():
            for source_path in source_paths:
                ppdata.rsync_copy(source_path, destination)
                set_move_table(s, dbi, source_host, source_path, dest_host, dest_path)
                shutil.rmtree(source_path)
        else:
            with ppdata.ssh_scope(source_host) as ssh:
                for source_path in source_paths:
                    rsync_copy_command = '''rsync -ac {source_path} {destination}'''.format(source_path=source_path, destination=destination)
                    rsync_del_command = '''rm -r {source_path}'''.format(source_path=source_path)
                    ssh.exec_command(rsync_copy_command)
                    set_move_table(s, dbi, source_host, source_path, dest_host, dest_path)
                    ssh.exec_command(rsync_del_command)
    print('Completed transfer')

if __name__ == '__main__':
    dbi = pdbi.DataBaseInterface()
    move_files(dbi)
