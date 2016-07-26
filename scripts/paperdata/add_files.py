'''
scripts.paperdata.add_files

pulls info about files and their related observations, then adds to the paperdata database

author | Immanuel Washington
'''
from __future__ import print_function
import argparse
import glob
from paper.data import dbi as pdbi, add

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Add files to the database')
    parser.add_argument('--source_host', type=str, help='source host')
    parser.add_argument('--source_paths', type=str, help='source paths')

    args = parser.parse_args()

    source_host = pdbi.hostnames.get(args.source_host, args.source_host)
    source_paths = glob.glob(args.source_paths)

    add.add_files(source_host, source_paths)
