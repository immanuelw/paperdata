# We need a key to use Sessions (which aren't really being used yet, but we will need them in the future).
SECRET_KEY = "secretkey"

import os
basedir = os.path.abspath(os.path.dirname(__file__))

SQLALCHEMY_DATABASE_URI = os.environ['DATABASE_URL']
SQLALCHEMY_MIGRATE_REPO = os.path.join(basedir, 'migrations')
