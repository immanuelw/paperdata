'''
paper.data.scripts.test_mod

runs tests for doctests of paper pipeline

author | Immanuel Washington

Functions
---------
module_test | runs doctests on modules
'''
from __future__ import print_function
import doctest
from paper.data import uv_data, file_data
from paper import convert

def module_test():
    '''
    runs doctest on modules
    '''
    print('testing modules')
    doctest.testmod(convert)
    doctest.testmod(file_data)
    doctest.testmod(uv_data)

if __name__ == '__main__':
    module_test()
