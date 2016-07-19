'''
paper.data.delete

delete info from database

author | Immanuel Washington

Functions
---------
delete_check | checks for which files should and can be deleted
set_delete_table | updates database with deleted file status
delete_files | parses list of files then copies and deletes them
'''
from __future__ import print_function
import os
import datetime
import shutil
import socket
import uuid
import paper as ppdata
from paper.data import dbi as pdbi

def delete_check(s, source_host):
    '''
    checks for which files can be deleted

    Parameters
    ----------
    s | object: session object
    source_host | str: host of system

    Returns
    -------
    list[str]: uv* file paths of files to be deleted
    '''
    table = pdbi.File
    FILEs = s.query(table).filter(table.delete_file == True).filter(table.tape_index != None).filter(table.host == source_host).all()
    paths = tuple(os.path.join(FILE.base_path, FILE.filename) for FILE in FILEs)

    return paths

def set_delete_table(s, source_host, source_path, dest_host, dest_path):
    '''
    updates table for deleted file

    Parameters
    ----------
    s | object: session object
    source_host | str: user host
    source_path | str: source file path
    dest_host | str: output host
    dest_path | str: output directory
    '''
    source = ':'.join((source_host, source_path))
    timestamp = datetime.datetime.utcnow()
    FILE = s.query(pdbi.File).get(source)
    delete_dict = {'host': dest_host,
                   'base_path': dest_path,
                   'is_deletable': False,
                   'timestamp': timestamp}
    for field, value in delete_dict.items():
        setattr(FILE, field, value)

    log_data = {'action': 'delete',
                'table': 'file',
                'identifier': FILE.source,
                'log_id': str(uuid.uuid4()),
                'timestamp': timestamp}
    s.add(pdbi.Log(**log_data))

def delete_files(s, source_host, source_paths, dest_host, dest_path):
    '''
    delete files

    Parameters
    ----------
    s | object: session object
    source_host | str: user host
    source_paths | list[str]: file paths
    dest_host | str: output host
    dest_path | str: output directory
    '''
    destination = ':'.join((dest_host, dest_path))
    out_host = socket.gethostname()
    if source_host == pdbi.hostnames.get(out_host, out_host):
        for source_path in source_paths:
            ppdata.rsync_copy(source_path, destination)
            set_delete_table(s, source_host, source_path, dest_host, dest_path)
            shutil.rmtree(source)
    else:
        with ppdata.ssh_scope(source_host) as ssh:
            for source_path in source_paths:
                rsync_copy_command = '''rsync -ac {source_path} {destination}'''.format(source_path=source_path, destination=destination)
                rsync_del_command = '''rm -r {source_path}'''.format(source_path=source_path)
                ssh.exec_command(rsync_copy_command)
                set_delete_table(s, source_host, source_path, dest_host, dest_path)
                ssh.exec_command(rsync_del_command)
    print('Completed transfer')

if __name__ == '__main__':
    print('This is just a module')
