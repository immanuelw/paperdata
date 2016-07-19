'''
paper.data.scripts.move_files

moves files and updates database with new host and path

author | Immanuel Washington
'''
from __future__ import print_function
import argparse
from paper.data import dbi as pdbi, move

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Move files, update database')
    parser.add_argument('--source_host', type=str, help='source host')
    parser.add_argument('--source_path', type=str, help='source path')
    parser.add_argument('--dest_host', type=str, help='destination host')
    parser.add_argument('--dest_path', type=str, help='destination path')
    parser.add_argument('--uname', type=str, help='host username')
    parser.add_argument('--pword', type=str, help='host password')

    args = parser.parse_args()

    source_host = pdbi.hostnames.get(args.source_host, args.source_host)
    dest_host = pdbi.hostnames.get(args.dest_host, args.dest_host)

    dbi = pdbi.DataBaseInterface()
    move.move_files(s,
                    source_host=source_host, source_paths_str=args.source_path,
                    dest_host=dest_host, dest_path=args.dest_path,
                    username=args.uname, password=args.pword)
