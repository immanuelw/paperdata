from flask.ext.script import Manager
from flask.ext.migrate import Migrate, MigrateCommand

from paper.site.admin.flask_app import app, db

migrate = Migrate(app, db)
manager = Manager(app)

manager.add_command('db', MigrateCommand)

from paper.site.admin.user_command import UserCommand
manager.add_command('user', UserCommand)

if __name__ == '__main__':
	manager.run()
