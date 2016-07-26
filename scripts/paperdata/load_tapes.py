'''
scripts.paperdata.load_tapes

loads files into tapes and updates their tape_index

author | Immanuel Washington
'''
from __future__ import print_function
import argparse
import glob
from paper.data import dbi as pdbi, add

if __name__ == '__main__':
    dbi = pdbi.DataBaseInterface()
    file_table = pdbi.File
    with dbi.session_scope() as s:
        FILEs = s.query(file_table)\
        		 .filter(file_table.tape_index != None)
        		 .all()

        #for FILE in FILEs:
        	#tape_index = load_into_tape(FILE)
        	#FILE.tape_index = tape_index
