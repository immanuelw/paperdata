'''
paper.data.scripts.backup_dbs

backups paper databases into json files

author | Immanuel Washington
'''
from __future__ import print_function
from paper import backup

if __name__ == '__main__':
    backup.backup_db(db='paperdata')
    backup.backup_db(db='paperdistiller')
    backup.backup_db(db='ganglia')
