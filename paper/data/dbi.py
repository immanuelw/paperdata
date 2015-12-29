'''
paper.data.dbi

author | Immanuel Washington

Classes
-------
Observation | sqlalchemy table
File | sqlalchemy table
Feed | sqlalchemy table
Log | sqlalchemy table
RTPFile | sqlalchemy table
RTPObservation | sqlalchemy table
RTPLog | sqlalchemy table
DataBaseInterface | interface to data database
'''
from sqlalchemy import Table, Column, String, Integer, ForeignKey, Float, func, Boolean, DateTime, Enum, BigInteger, Numeric, Text
from sqlalchemy import event, DDL
from sqlalchemy.orm import relationship, backref
import logging
import paper as ppdata

Base = ppdata.Base
logger = logging.getLogger('paper.data')

#########
#
#   Useful helper functions
#
#########

str_to_pol = {  'I' :  1,   # Stokes Paremeters
                'Q' :  2,
                'U' :  3,
                'V' :  4,
                'rr': -1,   # Circular Polarizations
                'll': -2,
                'rl': -3,
                'lr': -4,
                'xx': -5,   # Linear Polarizations
                'yy': -6,
                'xy': -7,
                'yx': -8}

pol_to_str = {v: k for k, v in str_to_pol.items()}

#############
#
#   The basic definition of our database
#
#############

class Observation(Base, ppdata.DictFix):
    __tablename__ = 'Observation'
    obsnum = Column(BigInteger, primary_key=True)
    julian_date = Column(Numeric(12,5))
    polarization = Column(String(4))
    julian_day = Column(Integer)
    lst = Column(Numeric(3,1))
    era = Column(Integer)
    era_type = Column(String(20))
    length = Column(Numeric(6,5)) #length of observation in fraction of a day
    ###
    time_start = Column(Numeric(12,5))
    time_end = Column(Numeric(12,5))
    delta_time = Column(Numeric(12,5))
    prev_obs = Column(BigInteger, unique=True)
    next_obs = Column(BigInteger, unique=True)
    is_edge = Column(Boolean)
    timestamp = Column(BigInteger)

class File(Base, ppdata.DictFix):
    __tablename__ = 'File'
    host = Column(String(100))
    base_path = Column(String(100)) #directory
    filename = Column(String(100)) #zen.*.*.uv/uvcRRE/uvcRREzx...
    filetype = Column(String(20)) #uv, uvcRRE, etc.
    source = Column(String(200), primary_key=True)
    ###
    obsnum = Column(BigInteger, ForeignKey('Observation.obsnum'))
    filesize = Column(Numeric(7,2))
    md5sum = Column(String(32))
    tape_index = Column(String(100))
    ### maybe unnecessary fields
    init_host = Column(String(100))
    is_tapeable = Column(Boolean)
    is_deletable = Column(Boolean)
    timestamp = Column(BigInteger)
    #this next line creates an attribute Observation.files which is the list of all
    #  files associated with this observation
    observation = relationship(Observation, backref=backref('files', uselist=True))

class Feed(Base, ppdata.DictFix):
    __tablename__ = 'Feed'
    host = Column(String(100))
    base_path = Column(String(100)) #directory
    filename = Column(String(100)) #zen.*.*.uv
    source = Column(String(200), primary_key=True)
    julian_day = Column(Integer)
    is_movable = Column(Boolean)
    is_moved = Column(Boolean)
    timestamp = Column(BigInteger)

class Log(Base, ppdata.DictFix):
    __tablename__ = 'Log'
    action = Column(String(100), nullable=False)
    table = Column(String(100))
    identifier = Column(String(200)) #the primary key that is used in other tables of the object being acted on
    log_id = Column(String(36), primary_key=True)
    timestamp = Column(BigInteger)

#def RTPFile(Base, ppdata.DictFix):
#   __tablename__ = 'RTPFile'
#   host = Column(String(100), nullable=False)
#   base_path = Column(String(100), nullable=False) #directory
#   filename = Column(String(100), nullable=False) #zen.*.*.uv/uvcRRE/uvcRREzx...
#   filetype = Column(String(20), nullable=False) #uv, uvcRRE, etc.
#   source = Column(String(200), primary_key=True)
#   obsnum = Column(BigInteger, ForeignKey('RTPObservation.obsnum'))
#   filesize = Column(Numeric(7,2))
#   md5sum = Column(String(32))
#   is_transferred = Column(Boolean)
#   julian_day = Column(Integer)
#   new_host = Column(String(100))
#   new_path = Column(String(100))
#   timestamp = Column(BigInteger)
#   observation = relationship(RTPObservation, backref=backref('files', uselist=True))

#class RTPObservation(Base, ppdata.DictFix):
#   __tablename__ = 'RTPObservation'
#   obsnum = Column(BigInteger, primary_key=True)
#   julian_date = Column(Numeric(12,5))
#   polarization = Column(String(4))
#   julian_day = Column(Integer)
#   era = Column(Integer)
#   length = Column(Numeric(6,5)) #length of RTPObservation in fraction of a day
#   prev_obs = Column(BigInteger, unique=True)
#   next_obs = Column(BigInteger, unique=True)
#   timestamp = Column(BigInteger)

#class RTPLog(Base, ppdata.DictFix):
#   __tablename__ = 'RTPLog'
#   action = Column(String(100), nullable=False)
#   table = Column(String(100))
#   identifier = Column(String(200)) #the primary key that is used in other tables of the object being acted on
#   log_id = Column(String(36), primary_key=True)
#   timestamp = Column(BigInteger)

class DataBaseInterface(ppdata.DataBaseInterface):
    '''
    Database Interface

    Methods
    -------
    create_db | creates all defined tables
    drop_db | drops all tables from database
    add_entry_dict | adds entry to database using dict as kwarg
    get_entry | gets database object
    '''
    def __init__(self, configfile='~/paperdata/paperdata.cfg'):
        '''
        Unique Interface for the paperdata database

        Parameters
        ----------
        configfile | str: paperdata database configuration file
        '''
        super(DataBaseInterface, self).__init__(configfile=configfile)

    def create_db(self):
        '''
        creates the tables in the database
        '''
        Base.metadata.bind = self.engine
        insert_update_trigger = DDL('''CREATE TRIGGER insert_update_trigger \
                                        after INSERT or UPDATE on file \
                                        FOR EACH ROW \
                                        SET NEW.source = concat(NEW.host, ':', NEW.base_path, '/', NEW.filename)''')
        event.listen(File.__table__, 'after_create', insert_update_trigger)
        Base.metadata.create_all()

    def drop_db(self, Base):
        '''
        drops tables in the database

        Parameters
        ----------
        Base | object: Base database object
        '''
        super(DataBaseInterface, self).drop_db(Base)

    def add_entry_dict(self, s, TABLE, entry_dict):
        '''
        create a new entry.

        Parameters
        ----------
        s | object: session object
        TABLE | str: table name
        entry_dict | dict: dict of attributes for object
        '''
        super(DataBaseInterface, self).add_entry_dict(__name__, s, TABLE, entry_dict)

    def get_entry(self, s, TABLE, unique_value):
        '''
        retrieves any object.
        Errors if there are more than one of the same object in the db. This is bad and should
        never happen

        Parameters
        ----------
        s | object: session object
        TABLE | str: table name
        unique_value | int/float/str: primary key value of row

        Returns
        -------
        object: table object
        '''
        super(DataBaseInterface, self).get_entry(__name__, s, TABLE, unique_value)
