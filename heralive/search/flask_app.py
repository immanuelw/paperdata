'''
heralive.search.flask_app

author | Immanuel Washington

Functions
---------
search_app | creates flask app for search
'''
from flask import Flask

search_app = Flask(__name__, static_folder='static', template_folder='templates')
#search_app = Flask(__name__, static_folder='search/static', template_folder='search/templates')
