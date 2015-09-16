from sqlalchemy import Table, Column, String, Integer, ForeignKey, Float, func, Boolean, DateTime, Enum, BigInteger, Numeric, Text
from sqlalchemy import event, DDL, UniqueConstraint, PrimaryKeyConstraint
from sqlalchemy.orm import relationship, backref
import os, sys, logging
import paper as ppdata

Base = ppdata.Base
logger = logging.getLogger('data')

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

class Observation(Base):
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

	def to_jsson(self):
	    new_dict = {}
	    for column in self.__table__.columns:
	        new_dict[column.name] = str(getattr(self, column.name))
	    return new_dict

class File(Base):
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

	def to_json(self):
		self.file_data = {'host':self.host,
						'path':self.path,
						'filename':self.filename,
						'filetype':self.filetype,
						'full_path':self.full_path,
						'obsnum':self.obsnum,
						'filesize':self.filesize,
						'md5sum':self.md5sum,
						'tape_index':self.tape_index,
						'source_host':self.source_host,
						'write_to_tape':self.write_to_tape,
						'delete_file':self.delete_file,
						'timestamp':self.timestamp}
		return self.file_data

class Feed(Base):
	__tablename__ = 'feed'
	host = Column(String(100))
	path = Column(String(100)) #directory
	filename = Column(String(100)) #zen.*.*.uv
	full_path = Column(String(200), primary_key=True)
	julian_day = Column(Integer)
	ready_to_move = Column(Boolean)
	moved_to_distill = Column(Boolean)
	timestamp = Column(BigInteger)

	def to_json(self):
		self.feed_data = {'host':self.host,
						'path':self.path,
						'filename':self.filename,
						'full_path':self.full_path,
						'julian_day':self.julian_day,
						'ready_to_move':self.ready_to_move,
						'moved_to_distill':self.moved_to_distill,
						'timestamp':self.timestamp}
		return self.feed_data

class Log(Base):
	__tablename__ = 'log'
	#__table_args__ = (PrimaryKeyConstraint('action', 'identifier', 'timestamp', name='action_time'),)
	action = Column(String(100), nullable=False)
	table = Column(String(100))
	identifier = Column(String(200)) #the primary key that is used in other tables of the object being acted on
	action_time = Column(String(200), primary_key=True)
	timestamp = Column(BigInteger)

	def to_json(self):
		self.log_data = {'action':self.action,
						'table':self.table,
						'identifier':self.identifier,
						'action_time':self.action_time,
						'timestamp':self.timestamp}
		return log_data

#def Rtp_File(Base):
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

#	def to_json(self):
#		self.rtp_file_data = {'host':self.host,
#							'path':self.path,
#							'filename':self.filename,
#							'filetype':self.filetype,
#							'full_path':self.full_path,
#							'obsnum':self.obsnum,
#							'filesize':self.filesize,
#							'md5sum':self.md5sum,
#							'transferred':self.transferred,
#							'julian_day':self.julian_day,
#							'new_host':self.new_host,
#							'new_path':self.new_path,
#							'timestamp':self.timestamp}
#		return self.rtp_data

#class Rtp_Observation(Base):
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

#	def to_json(self):
#		self.rtp_obs_data = {'obsnum':self.obsnum,
#							'julian_date':self.julian_date,
#							'polarization':self.polarization,
#							'julian_day':self.julian_day,
#							'era':self.era,
#							'length':self.length,
#							'prev_obs':self.prev_obs, 
#							'next_obs':self.next_obs,
#							'timestamp':self.timestamp}
#		return self.rtp_obs_data

#class Rtp_Log(Base):
#	__tablename__ = 'rtp_log'
#	__table_args__ = (PrimaryKeyConstraint('action', 'identifier', 'timestamp', name='action_time'),)
#	action = Column(String(100), nullable=False)
#	table = Column(String(100))
#	identifier = Column(String(200)) #the primary key that is used in other tables of the object being acted on
#	timestamp = Column(BigInteger)
#
#	def to_json(self):
#		self.rtp_log_data = {'action':self.action,
#							'table':self.table,
#							'identifier':self.identifier,
#							'timestamp':self.timestamp}
#		return rtp_log_data


class DataBaseInterface(ppdata.DataBaseInterface):
	def __init__(self):
		super(DataBaseInterface, self).__init__(configfile='~/paper.cfg', test=False)

	def create_db(self):
		"""
		creates the tables in the database.
		"""
		Base.metadata.bind = self.engine
		insert_update_trigger = DDL('''CREATE TRIGGER insert_update_trigger \
										after INSERT or UPDATE on file \
										FOR EACH ROW \
										SET NEW.full_path = concat(NEW.host, ':', NEW.path, '/', NEW.filename)''')
		event.listen(File.__table__, 'after_create', insert_update_trigger)
		Base.metadata.create_all()

	def drop_db(self):
		"""
		drops the tables in the database.
		"""
		Base.metadata.bind = self.engine
		Base.metadata.drop_all()

	def add_to_table(self, TABLE, entry_dict):
		"""
		create a new entry.
		"""
		table = getattr(sys.modules[__name__], TABLE.title())
		if TABLE in ('observation', 'feed', 'log', 'rtp_file', 'rtp_observation', 'rtp_log'):
			ENTRY = table(**entry_dict)
		elif TABLE in ('file',):
			#files linked to observations
			obs_table = getattr(sys.modules[__name__], 'Observation')
			ENTRY = table(**entry_dict)
			#get the observation corresponding to this file
			s = self.Session()
			OBS = s.query(obs_table).get(entry_dict['obsnum'])
			setattr(ENTRY, 'observation', OBS)  #associate the file with an observation
			s.close()
		self.add_entry(ENTRY)
		return None
