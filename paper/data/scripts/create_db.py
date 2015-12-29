'''
paper.data.scripts.create_db

creates tables outlined in database

author | Immanuel Washington
'''
from paper.data import dbi as pdbi

if __name__ == '__main__':
    dbi = pdbi.DataBaseInterface()
    dbi.create_db()
