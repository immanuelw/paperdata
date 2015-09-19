from flask import Flask
from flask.ext.sqlalchemy import SQLAlchemy
from flask.ext.login import LoginManager

app = Flask(__name__)
app.config.from_pyfile('settings.py')

lm = LoginManager()
lm.init_app(app)

db = SQLAlchemy(app)
