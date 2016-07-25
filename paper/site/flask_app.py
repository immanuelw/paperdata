'''
paper.site.flask_app

author | Immanuel Washington

Functions
---------
search_app | creates flask app for search db
'''
from flask import Flask
from flask.ext.sqlalchemy import SQLAlchemy
from flask.ext.login import LoginManager

search_app = Flask(__name__, static_folder='search/static', template_folder='search/templates')
