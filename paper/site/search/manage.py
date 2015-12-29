'''
paper.site.search.manage

author | Immanuel Washington

Functions
---------
migrate | migrates database
manager | manages commands for database and user
'''
from flask.ext.script import Manager
from flask.ext.migrate import Migrate, MigrateCommand
from paper.site.flask_app import search_app as app, search_db as db

migrate = Migrate(app, db)
manager = Manager(app)

manager.add_command('db', MigrateCommand)

from paper.site.user_command import UserCommand
manager.add_command('user', UserCommand)

if __name__ == '__main__':
    manager.run()
