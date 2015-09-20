from flask.ext.script import Manager
from flask.ext.migrate import Migrate, MigrateCommand

from paper.site.flask_app import search_app as app, search_db as db

migrate = Migrate(app, db)
manager = Manager(app)

manager.add_command('db', MigrateCommand)

from paper.site.search.user_command import UserCommand
manager.add_command('user', UserCommand)

if __name__ == '__main__':
	manager.run()
