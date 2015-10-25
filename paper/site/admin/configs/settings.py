'''
paper.site.admin.settings

author | Immanuel Washington
'''
import os
SECRET_KEY = 'secretkey'

basedir = os.path.abspath(os.path.dirname(__file__))

SQLALCHEMY_DATABASE_URI = 'postgres://username:password@host:port/db'
SQLALCHEMY_MIGRATE_REPO = os.path.join(basedir, 'migrations')
