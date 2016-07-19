'''
paper.distiller.dbi

author | Immanuel Washington

Classes
-------
File | sqlalchemy table
Observation | sqlalchemy table
Neighbors | sqlalchemy table
Log | sqlalchemy table
DataBaseInterface | interface to paperdistiller database
'''
from sqlalchemy import Table, Column, String, Integer, ForeignKey, Float, func, Boolean, DateTime, Enum, BigInteger, Numeric, Text
from sqlalchemy import event, DDL
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base
import datetime
import logging
import paper as ppdata

Base = declarative_base()
logger = logging.getLogger('paper.distiller')

FILE_PROCESSING_STAGES = ('NEW', 'UV_POT', 'UV', 'UVC', 'CLEAN_UV', 'UVCR', 'CLEAN_UVC',
                          'ACQUIRE_NEIGHBORS', 'UVCRE', 'NPZ', 'UVCRR', 'NPZ_POT', 'CLEAN_UVCRE', 'UVCRRE',
                          'CLEAN_UVCRR', 'CLEAN_NPZ', 'CLEAN_NEIGHBORS', 'UVCRRE_POT', 'CLEAN_UVCRRE', 'CLEAN_UVCR','COMPLETE')`
#############
#
#   The basic definition of our database
#
#############

class File(Base, ppdata.DictFix):
    __tablename__ = 'file'
    filenum = Column(Integer, primary_key=True, doc='id for file')
    filename = Column(String(100), doc='full file path')
    host = Column(String(100), doc='host that file is located on')
    obsnum = Column(BigInteger, ForeignKey('observation.obsnum'), doc='foreign key to observation table')
    observation = relationship(Observation, backref=backref('files', uselist=True))
    md5sum = Column(Integer, doc='md5 checksum of visdata')

class Observation(Base, ppdata.DictFix):
    __tablename__ = 'observation'
    julian_date = Column(Numeric(16, 8), doc='julian date of observation')
    pol = Column(String(4), doc='polarization of observation')
    obsnum = Column(BigInteger, primary_key=True, doc='observation id')
    status = Column(Enum(*FILE_PROCESSING_STAGES, name='FILE_PROCESSING_STAGES'), doc='compression stage of file')
    length = Column(Float, doc='length of observation in fraction of a day')
    currentpid = Column(Integer, doc='current id of compression process')
    stillhost = Column(String(100), doc='host of file being compressed')
    stillpath = Column(String(100), doc='path of file being compressed')
    outputpath = Column(String(100), doc='path to output file to')
    outputhost = Column(String(100), doc='host to output file to')
    high_neighbors = relationship('Observation',
                                    secondary=neighbors,
                                    primaryjoin=obsnum==neighbors.c.low_neighbor_id,
                                    secondaryjoin=obsnum==neighbors.c.high_neighbor_id,
                                    backref='low_neighbors')

class Neighbors(Base, ppdata.DictFix):
    __tablename__ = 'neighbors'
    low_neighbor_id = Column(BigInteger, ForeignKey('observation.obsnum'), unique=True, doc='obsnum for lower edge of file')
    high_neighbor_id = Column(BigInteger, ForeignKey('observation.obsnum'), unique=True, doc='obsnum for higher edge of file')

class Log(Base, ppdata.DictFix):
    __tablename__ = 'log'
    lognum = Column(Integer, primary_key=True, doc='log id')
    obsnum = Column(BigInteger, ForeignKey('observation.obsnum'), doc='foreign key to observation table')
    stage = Column(Enum(*FILE_PROCESSING_STAGES, name='FILE_PROCESSING_STAGES'), doc='compression state of file')
    exit_status = Column(Integer, doc='status value generated by scheduler')
    timestamp = Column(DateTime, nullable=False, default=func.current_timestamp(), doc='timestamp of log')
    logtext = Column(Text, doc='text of log -- the actual information')

class DataBaseInterface(ppdata.DataBaseInterface):
    '''
    Database Interface

    Methods
    -------
    create_db | creates all defined tables
    drop_db | drops all tables from database
    '''
    def __init__(self, Base=Base, configfile=ppdata.osj(ppdata.root_dir, 'config', 'paperdistiller.cfg')):
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
        self.Base.metadata.create_all()

    def drop_db(self):
        '''
        drops tables in the database

        Parameters
        ----------
        Base | object: Base database object
        '''
        super(DataBaseInterface, self).drop_db()
