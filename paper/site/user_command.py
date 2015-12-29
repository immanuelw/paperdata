'''
paper.site.user_command

author | Immanuel Washington

Functions
---------
create_user | creates user for website if does not exist
'''
from flask.ext.script import Command, Manager, Option
import hashlib
from sqlalchemy.exc import IntegrityError

UserCommand = Manager(usage='Perform user creation')

@UserCommand.option('-db', '--database', dest='database', default=None, help='Database to insert User into')
@UserCommand.option('-fn', '--firstname', dest='first_name', default=None, help='User\'s first name')
@UserCommand.option('-ln', '--lastname', dest='last_name', default=None, help='User\'s last name')
@UserCommand.option('-e', '--email', dest='email', default=None, help='User\'s email address')
@UserCommand.option('-u', '--username', dest='username', default=None, help='User\'s username. Used for logging in.')
@UserCommand.option('-p', '--password', dest='password', default=None, help='User\'s password. Used for logging in.')
def create_user(database, first_name, last_name, email, username, password):
    '''
    creates user when called through command

    Parameters
    ----------
    database | str: database name
    first_name | str: first name of user
    last_name | str: last name
    email | str: email address
    username | str: username
    password | str: password
    '''
    if not database or not first_name or not last_name or not email or not username or not password:
        print('Please pass all required parameters.\n Usage: flask/bin/python3.4 -m search.manage user create_user -db <database> fn <first name> -ln <last name> -e <email> -u <username> -p <password>')
    else:
        if database == 'search':
            from paper.site.search import models
            from paper.site.flask_app import search_db as db
        elif database == 'admin':
            from paper.site.admin import models
            from paper.site.flask_app import admin_db as db
        password = password.encode('UTF-8')
        user = models.User(username=username, password=hashlib.sha512(password).hexdigest(), email=email,
                            firstname=first_name, lastname=last_name)
        db.session.add(user)

        try:
            db.session.commit()
            print('User created.')
        except IntegrityError:
            print('Could not create user (duplicate username).')
