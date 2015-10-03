from sqlalchemy import Table, Column, String, Integer, ForeignKey, Float, func, Boolean, DateTime, Enum, BigInteger, Numeric, Text
from sqlalchemy import event, DDL, UniqueConstraint, PrimaryKeyConstraint
from sqlalchemy.orm import relationship, backref
import os, sys, logging
import paper as ppdata

Base = ppdata.Base
logger = logging.getLogger('paper.data')

#########
#
#   Useful helper functions
#
#########

str2pol = {	'I' :  1,   # Stokes Paremeters
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

#############
#
#   The basic definition of our database
#
#############

class Observation(Base, ppdata.DictFix):
	__tablename__ = 'observation'
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
	edge = Column(Boolean)
	timestamp = Column(BigInteger)

class File(Base, ppdata.DictFix):
	__tablename__ = 'file'
	host = Column(String(100))
	path = Column(String(100)) #directory
	filename = Column(String(100)) #zen.*.*.uv/uvcRRE/uvcRREzx...
	filetype = Column(String(20)) #uv, uvcRRE, etc.
	full_path = Column(String(200), primary_key=True)
	###
	obsnum = Column(BigInteger, ForeignKey('observation.obsnum'))
	filesize = Column(Numeric(7,2))
	md5sum = Column(String(32))
	tape_index = Column(String(100))
	### maybe unnecessary fields
	source_host = Column(String(100))
	write_to_tape = Column(Boolean)
	delete_file = Column(Boolean)
	timestamp = Column(BigInteger)
	#this next line creates an attribute Observation.files which is the list of all
	#  files associated with this observation
	observation = relationship(Observation, backref=backref('files', uselist=True))

class Feed(Base, ppdata.DictFix):
	__tablename__ = 'feed'
	host = Column(String(100))
	path = Column(String(100)) #directory
	filename = Column(String(100)) #zen.*.*.uv
	full_path = Column(String(200), primary_key=True)
	julian_day = Column(Integer)
	ready_to_move = Column(Boolean)
	moved_to_distill = Column(Boolean)
	timestamp = Column(BigInteger)

class Log(Base, ppdata.DictFix):
	__tablename__ = 'log'
	#__table_args__ = (PrimaryKeyConstraint('action', 'identifier', 'timestamp', name='action_time'),)
	action = Column(String(100), nullable=False)
	table = Column(String(100))
	identifier = Column(String(200)) #the primary key that is used in other tables of the object being acted on
	action_time = Column(String(200), primary_key=True)
	timestamp = Column(BigInteger)

#def Rtp_File(Base, ppdata.DictFix):
#	__tablename__ = 'rtp_file'
#	host = Column(String(100), nullable=False)
#	path = Column(String(100), nullable=False) #directory
#	filename = Column(String(100), nullable=False) #zen.*.*.uv/uvcRRE/uvcRREzx...
#	filetype = Column(String(20), nullable=False) #uv, uvcRRE, etc.
#	full_path = Column(String(200), primary_key=True)
#	obsnum = Column(BigInteger, ForeignKey('rtp_observation.obsnum'))
#	filesize = Column(Numeric(7,2))
#	md5sum = Column(String(32))
#	transferred = Column(Boolean)
#	julian_day = Column(Integer)
#	new_host = Column(String(100))
#	new_path = Column(String(100))
#	timestamp = Column(BigInteger)
#	observation = relationship(Rtp_Observation, backref=backref('files', uselist=True))

#class Rtp_Observation(Base, ppdata.DictFix):
#	__tablename__ = 'rtp_observation'
#	obsnum = Column(BigInteger, primary_key=True)
#	julian_date = Column(Numeric(12,5))
#	polarization = Column(String(4))
#	julian_day = Column(Integer)
#	era = Column(Integer)
#	length = Column(Numeric(6,5)) #length of rtp_observation in fraction of a day
#	prev_obs = Column(BigInteger, unique=True)
#	next_obs = Column(BigInteger, unique=True)
#	timestamp = Column(BigInteger)

#class Rtp_Log(Base, ppdata.DictFix):
#	__tablename__ = 'rtp_log'
#	__table_args__ = (PrimaryKeyConstraint('action', 'identifier', 'timestamp', name='action_time'),)
#	action = Column(String(100), nullable=False)
#	table = Column(String(100))
#	identifier = Column(String(200)) #the primary key that is used in other tables of the object being acted on
#	timestamp = Column(BigInteger)

class DataBaseInterface(ppdata.DataBaseInterface):
	def __init__(self, configfile='~/paperdata.cfg'):
		'''
		Unique Interface for the paperdata database

		Args:
			configfile (str): paperdata database configuration file
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
										SET NEW.full_path = concat(NEW.host, ':', NEW.path, '/', NEW.filename)''')
		event.listen(File.__table__, 'after_create', insert_update_trigger)
		Base.metadata.create_all()

	def drop_db(self, Base):
		'''
		drops tables in the database

		Args:
			object: Base database object
		'''
		super(DataBaseInterface, self).drop_db(Base)

	def add_entry_dict(self, s, TABLE, entry_dict):
		'''
		create a new entry.

		Args:
			s (object): session object
			TABLE (str): table name
			entry_dict (dict): dict of attributes for object
		'''
		super(DataBaseInterface, self).add_entry_dict(__name__, s, TABLE, entry_dict)

	def get_entry(self, s, TABLE, unique_value):
		'''
		retrieves any object.
		Errors if there are more than one of the same object in the db. This is bad and should
		never happen

		Args:
			s (object): session object
			TABLE (str): table name
			unique_value (int/float/str): primary key value of row

		Returns:
			object: table object
		'''
		super(DataBaseInterface, self).get_entry(__name__, s, TABLE, unique_value)
