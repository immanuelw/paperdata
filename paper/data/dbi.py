'''
paper.data.dbi

author | Immanuel Washington

Classes
-------
Observation | sqlalchemy table
File | sqlalchemy table
Feed | sqlalchemy table
Log | sqlalchemy table
DataBaseInterface | interface to data database
'''
from sqlalchemy import Table, Column, String, Integer, ForeignKey, Float, func, Boolean, DateTime, Enum, BigInteger, Numeric, Text
from sqlalchemy import event, DDL
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base
import os
import datetime
import logging
import paper as ppdata

Base = declarative_base()
logger = logging.getLogger('paper.data')

#########
#
#   Useful helper functions
#
#########

str_to_pol = {'I' :  1,   # Stokes Paremeters
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
              'yx': -8,
              'all': -6}

pol_to_str = {v: k for k, v in str_to_pol.items()}

filetypes = ('uv', 'uvcRRE', 'npz')
eras = (32, 64, 128)

hosts_file = ppdata.osj(ppdata.root_dir, 'config', 'hostnames.txt')
with open(hosts_file, 'r') as hf:
    hosts = (host.strip().split('|') for host in hf)
    hostnames = {abbr: full_name for abbr, full_name in hosts}

#############
#
#   The basic definition of our database
#
#############

class Observation(Base, ppdata.DictFix):
    __tablename__ = 'Observation'
    obsnum = Column(BigInteger, primary_key=True, doc='observation number, unique through algorithm')
    julian_date = Column(Numeric(12,5), doc='julian date of observation')
    polarization = Column(Enum(*str_to_pol.keys(), name='polarizations'), doc='polarization of observation')
    julian_day = Column(Integer, doc='integer part of julian date')
    lst = Column(Numeric(3,1), doc='local sidereal time for South Africa at julian date')
    era = Column(Enum(*eras, name='eras'), doc='era of observation')
    era_type = Column(String(20), doc='type of observation taken, ex:dual pol')
    length = Column(Numeric(6,5), doc='length of observation in fraction of days')
    time_start = Column(Numeric(12,5), doc='start time of observation')
    time_end = Column(Numeric(12,5), doc='end time of observation')
    delta_time = Column(Numeric(12,5), doc='time step of observation')
    prev_obs = Column(BigInteger, unique=True, doc='observation number of previous observation')
    next_obs = Column(BigInteger, unique=True, doc='observation number of next observation')
    is_edge = Column(Boolean, doc='is observation at beginning or end of session')
    timestamp = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, doc='Time entry was last updated')

class File(Base, ppdata.DictFix):
    __tablename__ = 'File'
    host = Column(Enum(*hostnames.values(), name='hostnames'), doc='hostname of resident filesystem')
    base_path = Column(String(100), doc='directory file is located in')
    filename = Column(String(100), doc='filename')
    filetype = Column(Enum(*filetypes, name='filetypes'), doc='filetype')
    source = Column(String(200), primary_key=True, doc='full path of file')
    obsnum = Column(BigInteger, ForeignKey('Observation.obsnum'), doc='Foreign Key to Observation table')
    filesize = Column(Numeric(7,2), doc='size of file in megabytes')
    md5sum = Column(String(32), doc='md5 checksum of visdata or file')
    tape_index = Column(String(100), doc='indexed location of file on tape')
    init_host = Column(String(100), doc='original host of file')
    is_tapeable = Column(Boolean, doc='is file written to tape')
    is_deletable = Column(Boolean, doc='can file be deleted from disk')
    timestamp = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, doc='Time entry was last updated')
    #this next line creates an attribute Observation.files which is the list of all
    #  files associated with this observation
    observation = relationship(Observation, backref=backref('files', uselist=True))

class Feed(Base, ppdata.DictFix):
    __tablename__ = 'Feed'
    host = Column(Enum(*hostnames.values(), name='hostnames'), doc='hostname of resident filesystem')
    base_path = Column(String(100), doc='directory file is located in')
    filename = Column(String(100), doc='filename')
    source = Column(String(200), primary_key=True, doc='full path of file')
    julian_day = Column(Integer, doc='integer value of julian date')
    is_movable = Column(Boolean, doc='can file be moved to different location')
    is_moved = Column(Boolean, doc='has file been moved to different location')
    timestamp = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, doc='Time entry was last updated')

class Log(Base, ppdata.DictFix):
    __tablename__ = 'Log'
    action = Column(String(100), nullable=False, doc='action taken by script')
    table = Column(String(100), doc='table script is acting on')
    identifier = Column(String(200), doc='key of item that was changed')
    log_id = Column(String(36), primary_key=True, doc='UUID generated id')
    timestamp = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, doc='Time entry was last updated')

class DataBaseInterface(ppdata.DataBaseInterface):
    '''
    Database Interface

    Methods
    -------
    create_db | creates all defined tables
    drop_db | drops all tables from database
    '''
    def __init__(self, Base=Base, configfile=ppdata.osj(ppdata.root_dir, 'config', 'paperdata.cfg')):
        '''
        Unique Interface for the paperdata database

        Parameters
        ----------
        Base | object: declarative database base
        configfile | str: paperdata database configuration file
        '''
        super(DataBaseInterface, self).__init__(Base=Base, configfile=configfile)

    def create_db(self):
        '''
        creates the tables in the database
        '''
        self.Base.metadata.bind = self.engine
        insert_update_trigger = DDL('''CREATE TRIGGER insert_update_trigger \
                                        after INSERT or UPDATE on file \
                                        FOR EACH ROW \
                                        SET NEW.source = concat(NEW.host, ':', NEW.base_path, '/', NEW.filename)''')
        event.listen(File.__table__, 'after_create', insert_update_trigger)
        self.Base.metadata.create_all()

    def drop_db(self):
        '''
        drops tables in the database

        Parameters
        ----------
        Base | object: Base database object
        '''
        super(DataBaseInterface, self).drop_db()
