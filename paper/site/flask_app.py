from flask import Flask
from flask.ext.sqlalchemy import SQLAlchemy
from flask.ext.login import LoginManager

search_app = Flask(__name__)
search_app.config.from_pyfile('search/settings.py')

search_lm = LoginManager()
search_lm.init_app(search_app)

search_db = SQLAlchemy(search_app)

admin_app = Flask(__name__)
admin_app.config.from_pyfile('admin/settings.py')

admin_lm = LoginManager()
admin_lm.init_app(admin_app)

admin_db = SQLAlchemy(admin_app)
