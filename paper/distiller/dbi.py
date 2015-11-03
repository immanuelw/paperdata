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
import logging
import paper as ppdata

Base = ppdata.Base
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
	filenum = Column(Integer, primary_key=True)
	filename = Column(String(100))
	host = Column(String(100))
	obsnum = Column(BigInteger, ForeignKey('observation.obsnum'))
	observation = relationship(Observation, backref=backref('files', uselist=True))
	md5sum = Column(Integer)

class Observation(Base, ppdata.DictFix):
	__tablename__ = 'observation'
	julian_date = Column(Numeric(16, 8))
	pol = Column(String(4))
	obsnum = Column(BigInteger, primary_key=True)
	status = Column(Enum(*FILE_PROCESSING_STAGES, name='FILE_PROCESSING_STAGES'))
	length = Column(Float) #length of observation in fraction of a day
	currentpid = Column(Integer)
	stillhost = Column(String(100))
	stillpath = Column(String(100))
	outputpath = Column(String(100))
	outputhost = Column(String(100))
	high_neighbors = relationship('Observation',
									secondary=neighbors,
									primaryjoin=obsnum==neighbors.c.low_neighbor_id,
									secondaryjoin=obsnum==neighbors.c.high_neighbor_id,
									backref='low_neighbors')

class Neighbors(Base, ppdata.DictFix):
	__tablename__ = 'neighbors'
	low_neighbor_id = Column(BigInteger, ForeignKey('observation.obsnum'), unique=True),
	high_neighbor_id = Column(BigInteger, ForeignKey('observation.obsnum'), unique=True)

class Log(Base, ppdata.DictFix):
	__tablename__ = 'log'
	lognum = Column(Integer, primary_key=True)
	obsnum = Column(BigInteger, ForeignKey('observation.obsnum'))
	stage = Column(Enum(*FILE_PROCESSING_STAGES, name='FILE_PROCESSING_STAGES'))
	exit_status = Column(Integer)
	timestamp = Column(DateTime, nullable=False, default=func.current_timestamp())
	logtext = Column(Text)

class DataBaseInterface(ppdata.DataBaseInterface):
	'''
	Database Interface

	Methods
	-------
	add_entry_dict | adds entry to database using dict as kwarg
	get_entry | gets database object
	'''
	def __init__(self, configfile='~/paperdata/paperdistiller.cfg'):
		'''
		Unique Interface for the paperdata database

		Parameters
		----------
		configfile | str: paperdata database configuration file
		'''
		super(DataBaseInterface, self).__init__(configfile=configfile)

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
