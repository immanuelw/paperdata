from sqlalchemy import Table, Column, String, Integer, ForeignKey, Float, func, Boolean, DateTime, Enum, BigInteger, Numeric, Text
from sqlalchemy import event, DDL, UniqueConstraint
from sqlalchemy.orm import relationship, backref, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool, QueuePool
import os, numpy as n, sys, logging
import configparser
#Based on example here: http://www.pythoncentral.io/overview-sqlalchemys-expression-language-orm-queries/
Base = declarative_base()
logger = logging.getLogger('paperdata_dbi')

dbinfo = {'username':'immwa',
		  'password':'\x69\x6d\x6d\x77\x61\x33\x39\x37\x38',
		  'hostip':'shredder',
		  'port':3306,
		  'dbname':'paperdata'}

#########
#
#   Useful helper functions
#
#########

#SSH/SFTP Function
#Need private key so don't need username/password
def login_ssh(host, username=None):
	ssh = paramiko.SSHClient()
	ssh.load_system_host_keys()
	ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	try:
		ssh.connect(host, username=username, key_filename='/home/{0}/.ssh/id_rsa'.format(username))
	except:
		try:
			ssh.connect(host, key_filename='/home/{0}/.ssh/id_rsa'.format(username))
		except:
			return None

	return ssh

#############
#
#   The basic definition of our database
#
#############

class Monitor(Base):
	__tablename__ = 'monitor'
	host = Column(String(100))
	path = Column(String(100))
	filename = Column(String(100))
	full_path = Column(String(200), unique=True)
	status = Column(String(100))
	del_time = Column(BigInteger)
	time_start = Column(BigInteger)
	time_end = Column(BigInteger)
	time_date = Column(Numeric(13,6))

class Ram(Base):
	__tablename__ = 'ram'

class Iostat(Base):
	__tablename__ = 'iostat'

class Cpu(Base):
	__tablename__ = 'cpu'

class DataBaseInterface(object):
	def __init__(self,configfile='./still.cfg',test=False):
		"""
		Connect to the database and initiate a session creator.
		 or
		create a FALSE database

		db.cfg is the default setup. Config files live in ddr_compress/configs
		To use a config file, copy the desired file ~/.paperstill/db.cfg
		"""
		if not configfile is None:
			config = configparser.ConfigParser()
			configfile = os.path.expanduser(configfile)
			if os.path.exists(configfile):
				logger.info('loading file '+configfile)
				config.read(configfile)
				self.dbinfo = config['dbinfo']
				self.dbinfo['password'] = self.dbinfo['password'].decode('string-escape')
			else:
				logging.info(configfile+" Not Found")
		if test:
			self.engine = create_engine('sqlite:///',
										connect_args={'check_same_thread':False},
										poolclass=StaticPool)
			self.create_db()
		else:
			self.engine = create_engine(
					'mysql://{username}:{password}@{hostip}:{port}/{dbname}'.format(
								**self.dbinfo),
								pool_size=20,
								max_overflow=40)
		self.Session = sessionmaker(bind=self.engine)

	def get_obs(self, obsnum):
		"""
		retrieves an observation object.
		Errors if there are more than one of the same obsnum in the db. This is bad and should
		never happen

		todo:test
		"""
		s = self.Session()
		OBS = s.query(Observation).filter(Observation.obsnum==obsnum).one()
		s.close()
		return OBS

	def update_obs(self, OBS):
		"""
		updates file object field
		***NEED TO TEST
		"""
		s = self.Session()
		s.add(OBS)
		s.commit()
		s.close()
		return True

	def get_file(self, full_path):
		"""
		retrieves an file object.
		Errors if there are more than one of the same file in the db. This is bad and should
		never happen

		todo:test
		"""
		s = self.Session()
		FILE = s.query(File).filter(File.full_path==full_path).one()
		s.close()
		return FILE

	def update_file(self, FILE):
		"""
		updates file object field
		***NEED TO TEST
		"""
		s = self.Session()
		s.add(FILE)
		s.commit()
		s.close()
		return True

	def get_feed(self, full_path):
		"""
		retrieves an feed object.
		Errors if there are more than one of the same feed in the db. This is bad and should
		never happen

		todo:test
		"""
		s = self.Session()
		FEED = s.query(Feed).filter(Feed.full_path==full_path).one()
		s.close()
		return FEED

	def update_feed(self, FEED):
		"""
		updates feed object field
		***NEED TO TEST
		"""
		s = self.Session()
		s.add(FEED)
		s.commit()
		s.close()
		return True

	def create_db(self):
		"""
		creates the tables in the database.
		"""
		Base.metadata.bind = self.engine
		table = File.__table__
		insert_update_trigger = DDL('''CREATE TRIGGER insert_update_tigger
										after INSERT or UPDATE on File
										FOR EACH ROW
										SET NEW.full_path = concat(NEW.host, ':', NEW.path, '/', NEW.filename)''')
		event.listen(table, 'after_create', insert_update_trigger)
		Base.metadata.create_all()

	def drop_db(self):
		"""
		drops the tables in the database.
		"""
		Base.metadata.bind = self.engine
		Base.metadata.drop_all()

	def add_observation(self, obsnum, julian_date, polarization, julian_day, era, era_type, length, time_start, time_end, delta_time,
						prev_obs, next_obs, edge):
		"""
		create a new observation entry.
		returns: obsnum  (see jdpol2obsnum)
		Note: does not link up neighbors!
		"""
		OBS = Observation(obsnum, julian_date, polarization, julian_day, era, era_type, length, time_start, time_end, delta_time,
							prev_obs, next_obs, edge)
		s = self.Session()
		s.add(OBS)
		s.commit()
		obsnum = OBS.obsnum
		s.close()
		sys.stdout.flush()
		return obsnum

	def add_file(self, host, path, filename, filetype, obsnum, filesize, md5, tape_index, write_to_tape, delete_file): #cal_path?? XXXX
		"""
		Add a file to the database and associate it with an observation.
		"""
		FILE = File(host, path, filename, filetype, obsnum, filesize, md5, tape_index, write_to_tape, delete_file) #cal_path?? XXXX
		#get the observation corresponding to this file
		s = self.Session()
		OBS = s.query(Observation).filter(Observation.obsnum==obsnum).one()
		FILE.observation = OBS  #associate the file with an observation
		s.add(FILE)
		s.commit()
		#filenum = FILE.filenum #we gotta grab this before we close the session.
		s.close() #close the session
		#return filenum
		return None

	def add_feed(self, host, path, filename, ready_to_move, moved_to_distill):
		"""
		Add a feed to the database
		"""
		FEED = Feed(self, host, path, filename, ready_to_move, moved_to_distill)
		s = self.Session()
		s.add(FEED)
		s.commit()
		s.close() #close the session
		return None

	def get_file_path(self, full_path):
		"""
		todo
		"""
		FILE = self.get_file(full_path)
		return FILE.path

	def set_file_path(self, full_path, path):
		"""
		todo
		"""
		FILE = self.get_file(full_path)
		FILE.path = path
		yay = self.update_file(FILE)
		return yay
