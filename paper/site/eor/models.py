'''
paper.site.search.models

author | Immanuel Washington

Classes
-------
SetSubscriptions | sqlalchemy table
DataSourceSubscriptions | sqlalchemy table
ActiveDataSources | sqlalchemy table
User | sqlalchemy table
Set | sqlalchemy table
FlaggedSubset | sqlalchemy table
FlaggedObsIds | sqlalchemy table
DataAmount | sqlalchemy table
Thread | sqlalchemy table
Comment | sqlalchemy table
GraphType | sqlalchemy table
GraphDataSource | sqlalchemy table
GraphDataSourceColumn | sqlalchemy table
'''
from paper.site.flask_app import search_db as db
from datetime import datetime

class SetSubscriptions(db.Model):
    __tablename__ = 'SetSubscriptions'
    username =  db.Column(db.String(32), db.ForeignKey('User.username'))
    set_id = db.Column(db.Integer, db.ForeignKey('Set.id'))
    id = db.Column(db.Integer, primary_key=True)

class DataSourceSubscriptions(db.Model):
    __tablename__ = 'DataSourceSubscriptions'
    username =  db.Column(db.String(32), db.ForeignKey('User.username'))
    data_source = db.Column(db.String(100), db.ForeignKey('GraphDataSource.name'))
    id = db.Column(db.Integer, primary_key=True)

class ActiveDataSources(db.Model):
    __tablename__ = 'ActiveDataSources'
    username =  db.Column(db.String(32), db.ForeignKey('User.username'))
    data_source = db.Column(db.String(100), db.ForeignKey('GraphDataSource.name'))
    id = db.Column(db.Integer, primary_key=True)

class User(db.Model):
    __tablename__ = 'User'
    username = db.Column(db.String(32), primary_key=True)
    # SHA-512 returns a 512-bit hash, which is 512 bits / 8 bits per byte * 2 hex digits per byte = 128 hex digits.
    password = db.Column(db.String(128), nullable=False)
    # 254 is the maximum length of an email address.
    email = db.Column(db.String(254), nullable=False)
    first_name = db.Column(db.String(50), nullable=False)
    last_name = db.Column(db.String(50), nullable=False)
    owned_sets = db.relationship('Set', backref='user')
    subscribed_sets = db.relationship('Set', secondary=SetSubscriptions.__table__)
    subscribed_data_sources = db.relationship('GraphDataSource', secondary=DataSourceSubscriptions.__table__)
    active_data_sources = db.relationship('GraphDataSource', secondary=ActiveDataSources.__table__)
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

class Set(db.Model):
    __tablename__ = 'Set'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(32), db.ForeignKey('User.username'))
    name = db.Column(db.String(50))
    start = db.Column(db.Integer)
    end = db.Column(db.Integer)
    polarization = db.Column(db.String(4)) #'all', 'xy', 'yy', etc.
    era_type = db.Column(db.String(10))
    host = db.Column(db.String(100))
    filetype = db.Column(db.String(100))
    total_data_hrs = db.Column(db.Float)
    flagged_data_hrs = db.Column(db.Float)
    created_on = db.Column(db.DateTime, default=datetime.utcnow)

class FlaggedSubset(db.Model):
    __tablename__ = 'FlaggedSubset'
    id = db.Column(db.Integer, primary_key=True)
    set_id = db.Column(db.Integer, db.ForeignKey('Set.id', ondelete='CASCADE'))
    start = db.Column(db.Integer)
    end = db.Column(db.Integer)

class FlaggedObsIds(db.Model):
    __tablename__ = 'FlaggedObsIds'
    id = db.Column(db.Integer, primary_key=True)
    obs_id = db.Column(db.Integer)
    flagged_subset_id = db.Column(db.Integer, db.ForeignKey('FlaggedSubset.id', ondelete='CASCADE'))

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
    thread_id = db.Column(db.Integer, db.ForeignKey('Thread.id'))
    text = db.Column(db.String(1000), nullable=False)
    username = db.Column(db.String(32))
    created_on = db.Column(db.DateTime, default=datetime.utcnow)

class GraphType(db.Model):
    __tablename__ = 'GraphType'
    name = db.Column(db.String(100), primary_key=True)

class GraphDataSource(db.Model):
    __tablename__ = 'GraphDataSource'
    name = db.Column(db.String(100), primary_key=True)
    graph_type = db.Column(db.String(100), db.ForeignKey('GraphType.name'))
    host = db.Column(db.String(100))
    database = db.Column(db.String(100))
    table = db.Column(db.String(100))
    obs_column = db.Column(db.String(100)) # Which column has the observation ids.
    width_slider = db.Column(db.Boolean) # Whether the graph should come with a column width slider.

class GraphDataSourceColumn(db.Model):
    __tablename__ = 'GraphDataSourceColumn'
    id = db.Column(db.Integer, primary_key=True)
    graph_data_source = db.Column(db.String(100), db.ForeignKey('GraphDataSource.name'))
    name = db.Column(db.String(100))
