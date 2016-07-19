'''
paper.data.scripts.distill_files

adds files to paperdistiller database

author | Immanuel Washington
'''
from __future__ import print_function
import argparse
import glob
from paper.data import distill

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Move files, update database')
    parser.add_argument('--path', type=str, help='paths')

    args = parser.parse_args()

    source_paths = glob.glob(args.path)
    distill.add_files(source_paths)
