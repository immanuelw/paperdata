#!/usr/bin/python
# -*- coding: utf-8 -*-
# Create paper tables

import dbi as dev
import add_files
import os
import socket

### Script to reload paper database
### Crawls all hosts for files

### Author: Immanuel Washington
### Date: 5-06-15

def find_paths(input_host):
    named_host = socket.gethostname()
    input_paths = []
    npz_paths = []
    if input_host == named_host:
        for root, dirs, files in os.walk('/'):
            for direc in dirs:
                if direc.endswith('uv'):
                    input_paths.append(os.path.join(root, direc))
                elif direc.endswith('uvcRRE'):
                    input_paths.append(os.path.join(root, direc))
            for file_path in files:
                if file_path.endswith('npz'):
                    npz_paths.append(os.path.join(root, file_path))
    else:
        ssh = dev.login_ssh(input_host)
        find = '''find / -name '*.uv' -o -name '*.uvcRRE' -o -name '*.npz' 2>/dev/null'''
        _, all_paths, _ = ssh.exec_command(find)
        for path in all_paths.split('\n'):
            if direc.endswith('uv'):
                 input_paths.append(path)
            elif direc.endswith('uvcRRE'):
                 input_paths.append(path)
            elif file_path.endswith('npz'):
                 npz_paths.append(path)
            
        ssh.close()         

    return (input_paths, npz_paths)

if __name__ == '__main__':
    if len(sys.argv) == 2:
        input_host = sys.argv[1]
    else:
        input_host = raw_input('Source directory host: ')

    input_paths, npz_paths = find_paths(input_host)
    input_paths = add_files.dupe_check(input_host, input_paths)
    input_paths.sort()
    npz_paths = add_files.dupe_check(input_host, npz_paths)
    npz_paths.sort()
    add_files.add_files(input_host, input_paths)
    add_files.add_files(input_host, npz_paths)
