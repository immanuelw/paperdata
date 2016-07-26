'''
scripts.paperdata.delete_files

copies files to cold storage and then deletes from main host

author | Immanuel Washington
'''
from __future__ import print_function
import argparse
from paper.data import dbi as pdbi, delete

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Delete files from the database')
    parser.add_argument('--source_host', type=str, help='source host')
    parser.add_argument('--dest_host', type=str, help='destination host')
    parser.add_argument('--dest_path', type=str, help='destination directory')

    args = parser.parse_args()

    source_host = pdbi.hostnames.get(args.source_host, args.source_host)
    dest_host = pdbi.hostnames.get(args.dest_host, args.dest_host)

    dbi = pdbi.DataBaseInterface()
    with dbi.session_scope() as s:
        source_paths = delete_check(s, source_host)
        delete.delete_files(s, source_host, source_paths,
                               dest_host, args.dest_path)
