from flask.ext.script import Command, Manager, Option
import hashlib
from sqlalchemy.exc import IntegrityError
from paper.site.admin.flask_app import db
from paper.site.admin import models

UserCommand = Manager(usage = 'Perform user creation')

@UserCommand.option('-fn', '--firstname', dest = 'first_name', default = None, help = 'User\'s first name')
@UserCommand.option('-ln', '--lastname', dest = 'last_name', default = None, help = 'User\'s last name')
@UserCommand.option('-e', '--email', dest = 'email', default = None, help = 'User\'s email address')
@UserCommand.option('-u', '--username', dest = 'username', default = None, help = 'User\'s username. Used for logging in.')
@UserCommand.option('-p', '--password', dest = 'password', default = None, help = 'User\'s password. Used for logging in.')
def create_user(first_name, last_name, email, username, password):
	if not first_name or not last_name or not email or not username or not password:
		print('Please pass all required parameters.\n Usage: flask/bin/python3.4 -m app.manage user create_user -fn <first name> -ln <last name> -e <email> -u <username> -p <password>')
	else:
		password = password.encode('UTF-8')
		user = getattr(models, 'User')(username=username, password=hashlib.sha512(password).hexdigest(), email=email,
							firstname=first_name, lastname=last_name)
		db.session.add(user)

		try:
			db.session.commit()
			print('User created.')
		except IntegrityError:
			print('Could not create user (duplicate username).')
