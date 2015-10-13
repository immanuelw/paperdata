'''
paper.site.flask_app

author | Immanuel Washington

Functions
---------
search_app | creates flask app for search db
search_lm | creates login manager for search db
search_db | creates search db from sqlalchemy
admin_app | creates flask app for admin db
admin_lm | creates login manager for admin db
admin_db | creates admin db from sqlalchemy
'''
from flask import Flask
from flask.ext.sqlalchemy import SQLAlchemy
from flask.ext.login import LoginManager

search_app = Flask(__name__, static_folder='search/static', template_folder='search/templates')
search_app.config.from_pyfile('search/settings.py')

search_lm = LoginManager()
search_lm.init_app(search_app)

search_db = SQLAlchemy(search_app)

admin_app = Flask(__name__, static_folder='admin/static', template_folder='admin/templates')
admin_app.config.from_pyfile('admin/settings.py')

admin_lm = LoginManager()
admin_lm.init_app(admin_app)

admin_db = SQLAlchemy(admin_app)
