'''
paper.site.admin.models

author | Immanuel Washington

Classes
-------
User | sqlalchemy table
DataAmount | sqlalchemy table
Thread | sqlalchemy table
Comment | sqlalchemy table
'''
from paper.site.flask_app import admin_db as db
from datetime import datetime

class User(db.Model):
    __tablename__ = 'User'
    username = db.Column(db.String(32), primary_key=True)
    # SHA-512 returns a 512-bit hash, which is 512 bits / 8 bits per byte * 2 hex digits per byte = 128 hex digits.
    password = db.Column(db.String(128), nullable=False)
    # 254 is the maximum length of an email address.
    email = db.Column(db.String(254), nullable=False)
    first_name = db.Column(db.String(50), nullable=False)
    last_name = db.Column(db.String(50), nullable=False)
    admin = db.Column(db.Boolean, default=False)

    def __init__(self, username, password, email, first_name, last_name):
        self.username = username
        self.password = password
        self.email = email
        self.first_name = first_name
        self.last_name = last_name

    def __repr__(self):
        return '<User %r>' % (self.username)

    def is_authenticated(self):
        return True

    def is_active(self):
        return True

    def is_anonymous(self):
        return False

    def get_id(self):
        return self.username

class DataAmount(db.Model):
    __tablename__ = 'DataAmount'
    # AUTO_INCREMENT is automatically set on the first Integer primary key column that is not marked as a foreign key.
    id = db.Column(db.Integer, primary_key=True)
    # Store a 'created_on' string field for the current time that is automatically inserted with a new entry into the database.
    # We're using UTC time, so that's why there is a Z at the end of the string.
    created_on = db.Column(db.DateTime, default=datetime.utcnow)
    hours_sadb = db.Column(db.Float)
    hours_paper = db.Column(db.Float)
    hours_with_data = db.Column(db.Float)
    data_transfer_rate = db.Column(db.Float)

    def to_json(self):
        data_dict = {'id': self.id,
                    'created_on': self.created_on,
                    'hours_sadb': round(self.hours_sadb or 0., 4),
                    'hours_paper': round(self.hours_paper or 0., 4),
                    'hours_with_data': round(self.hours_with_data or 0., 4),
                    'data_transfer_rate': round(self.data_transfer_rate or 0., 4)}
        return data_dict

class Thread(db.Model):
    __tablename__ = 'Thread'
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(100), nullable=False)
    username = db.Column(db.String(32))
    created_on = db.Column(db.DateTime, default=datetime.utcnow)
    last_updated = db.Column(db.DateTime, default=datetime.utcnow)

class Comment(db.Model):
    __tablename__ = 'Comment'
    id = db.Column(db.Integer, primary_key=True)
    thread_id = db.Column(db.Integer, db.ForeignKey('thread.id'))
    text = db.Column(db.String(1000), nullable=False)
    username = db.Column(db.String(32))
    created_on = db.Column(db.DateTime, default=datetime.utcnow)
