'''
paper.site.search.settings

author | Immanuel Washington
'''
import os
SECRET_KEY = 'secretkey'

basedir = os.path.abspath(os.path.dirname(__file__))

SQLALCHEMY_DATABASE_URI = 'postgres:///postgres'
SQLALCHEMY_MIGRATE_REPO = os.path.join(basedir, 'migrations')
