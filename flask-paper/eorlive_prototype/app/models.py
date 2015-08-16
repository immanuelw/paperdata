from app.flask_app import db
from datetime import datetime

class Set_Subscriptions(db.Model):
	__tablename__ = 'set_subscriptions'
    username =  db.Column(db.String(32), db.ForeignKey('user.username'))
    set_id = db.Column(db.Integer, db.ForeignKey('set.id'))

class Data_Source_Subscriptions(db.Model):
	__tablename__ = 'data_source_subscriptions'
    username =  db.Column(db.String(32), db.ForeignKey('user.username'))
    data_source = db.Column(db.String(100), db.ForeignKey('graph_data_source.name'))

class Active_Data_Sources(db.Model):
	__tablename__ = 'active_data_sources'
    username =  db.Column(db.String(32), db.ForeignKey('user.username'))
    data_source = db.Column(db.String(100), db.ForeignKey('graph_data_source.name'))

class User(db.Model):
    username = db.Column(db.String(32), primary_key=True)
    # SHA-512 returns a 512-bit hash, which is 512 bits / 8 bits per byte * 2 hex digits per byte = 128 hex digits.
    password = db.Column(db.String(128), nullable=False)
    # 254 is the maximum length of an email address.
    email = db.Column(db.String(254), nullable=False)
    first_name = db.Column(db.String(50), nullable=False)
    last_name = db.Column(db.String(50), nullable=False)
    owned_sets = db.relationship('Set', backref='user')
    subscribed_sets = db.relationship('Set', secondary='set_subscriptions')
    subscribed_data_sources = db.relationship('GraphDataSource', secondary='data_source_subscriptions')
    active_data_sources = db.relationship('GraphDataSource', secondary='active_data_sources')
    admin = db.Column(db.Boolean, default=False)

    def __init__(self, username, password, email, first_name, last_name):
        self.username = username;
        self.password = password;
        self.email = email;
        self.first_name = first_name;
        self.last_name = last_name;

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
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(32), db.ForeignKey('user.username'))
    name = db.Column(db.String(50))
    start = db.Column(db.Integer)
    end = db.Column(db.Integer)
    low_or_high = db.Column(db.String(4)) # Whether this set contains 'low', 'high', or 'any' observations.
    eor = db.Column(db.String(4)) # Whether this set contains 'EOR0', 'EOR1', or 'any' observations.
    total_data_hrs = db.Column(db.Float)
    flagged_data_hrs = db.Column(db.Float)
    created_on = db.Column(db.DateTime, default=datetime.utcnow)

class FlaggedSubset(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    set_id = db.Column(db.Integer, db.ForeignKey('set.id', ondelete="CASCADE"))
    start = db.Column(db.Integer)
    end = db.Column(db.Integer)

class FlaggedObsIds(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    obs_id = db.Column(db.Integer)
    flagged_subset_id = db.Column(db.Integer, db.ForeignKey('flagged_subset.id', ondelete="CASCADE"))

class DataAmount(db.Model):
    # AUTO_INCREMENT is automatically set on the first Integer primary key column that is not marked as a foreign key.
    id = db.Column(db.Integer, primary_key=True)
    # Store a 'created_on' string field for the current time that is automatically inserted with a new entry into the database.
    # We're using UTC time, so that's why there is a Z at the end of the string.
    created_on = db.Column(db.DateTime, default=datetime.utcnow)
    hours_scheduled = db.Column(db.Float)
    hours_observed = db.Column(db.Float)
    hours_with_data = db.Column(db.Float)
    hours_with_uvfits = db.Column(db.Float)
    data_transfer_rate = db.Column(db.Float)

    def asDict(self):
        return {
            'id': self.id,
            'created_on': self.created_on,
            'hours_scheduled': round(self.hours_scheduled or 0., 4),
            'hours_observed': round(self.hours_observed or 0., 4),
            'hours_with_data': round(self.hours_with_data or 0., 4),
            'hours_with_uvfits': round(self.hours_with_uvfits or 0., 4),
            'data_transfer_rate': round(self.data_transfer_rate or 0., 4)
        }

class Thread(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(100), nullable=False)
    username = db.Column(db.String(32))
    created_on = db.Column(db.DateTime, default=datetime.utcnow)
    last_updated = db.Column(db.DateTime, default=datetime.utcnow)

class Comment(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    thread_id = db.Column(db.Integer, db.ForeignKey('thread.id'))
    text = db.Column(db.String(1000), nullable=False)
    username = db.Column(db.String(32))
    created_on = db.Column(db.DateTime, default=datetime.utcnow)

class GraphType(db.Model):
    name = db.Column(db.String(100), primary_key=True)

class GraphDataSource(db.Model):
    name = db.Column(db.String(100), primary_key=True)
    graph_type = db.Column(db.String(100), db.ForeignKey('graph_type.name'))
    host = db.Column(db.String(100))
    database = db.Column(db.String(100))
    table = db.Column(db.String(100))
    obs_column = db.Column(db.String(100)) # Which column has the observation ids.
    projectid = db.Column(db.Boolean) # Whether the table has a projectid field.
    width_slider = db.Column(db.Boolean) # Whether the graph should come with a column width slider.

class GraphDataSourceColumn(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    graph_data_source = db.Column(db.String(100), db.ForeignKey('graph_data_source.name'))
    name = db.Column(db.String(100))
