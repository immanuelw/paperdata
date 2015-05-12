from sqlalchemy import Table, Column, String, Integer, ForeignKey, Float, func, Boolean, DateTime, Enum, BigInteger, Numeric, Text
from sqlalchemy import event, DDL, UniqueConstraint
from sqlalchemy.orm import relationship, backref, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool, QueuePool
import aipy as a, os, numpy as n, sys, logging
import configparser
import hashlib
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
def jdpol2obsnum(jd,pol,djd):
	"""
	input: julian date float, pol string. and length of obs in fraction of julian date
	output: a unique index
	"""
	dublinjd = jd - 2415020  #use Dublin Julian Date
	obsint = int(dublinjd/djd)  #divide up by length of obs
	polnum = a.miriad.str2pol[pol]+10
	assert(obsint < 2**31)
	return int(obsint + polnum*(2**32))

def updateobsnum(context):
	"""
	helper function for Observation sqlalchemy object.
	used to calculate the obsnum on creation of the record
	"""
	return jdpol2obsnum(context.current_parameters['julian_date'],
						context.current_parameters['polarization'],
						context.current_parameters['length'])

def get_md5sum(fname):
	"""
	calculate the md5 checksum of a file whose filename entry is fname.
	"""
	fname = fname.split(':')[-1]
	BLOCKSIZE=65536
	hasher=hashlib.md5()
	try:
		afile=open(fname, 'rb')
	except(IOError):
		afile=open("%s/visdata"%fname, 'rb')
	buf=afile.read(BLOCKSIZE)
	while len(buf) >0:
		hasher.update(buf)
		buf=afile.read(BLOCKSIZE)
	return hasher.hexdigest()

def gethostname():
	from subprocess import Popen,PIPE
	hn = Popen(['bash','-cl','hostname'], stdout=PIPE).communicate()[0].strip()
	return hn

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

class Observation(Base):
	__tablename__ = 'observation'
	obsnum = Column(BigInteger, default=updateobsnum, primary_key=True)
	julian_date = Column(Numeric(12,5))
	polarization = Column(String(4))
	julian_day = Column(Integer)
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

class File(Base):
	__tablename__ = 'file'
	filenum = Column(Integer, primary_key=True)
	host = Column(String(100))
	path = Column(String(100)) #directory
	filename = Column(String(100)) #zen.*.*.uv/uvcRRE/uvcRREzx...
	filetype = Column(String(20)) #uv, uvcRRE, etc.
	full_path = Column(String(200), unique=True)
	###
	obsnum = Column(BigInteger, ForeignKey('observation.obsnum'))
	filesize = Column(Numeric(7,2))
	md5sum = Column(Integer)
	tape_index = Column(String(100))
	### maybe unnecessary fields
	#calibration_path = Column(String(100))
	#history?
	write_to_tape = Column(Boolean)
	delete_file = Column(Boolean)
	#this next line creates an attribute Observation.files which is the list of all
	#  files associated with this observation
	observation = relationship(Observation, backref=backref('files', uselist=True))

class DataBaseInterface(object):
	def __init__(self,configfile='~/.ddr_compress/still.cfg',test=False):
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
		OBS = s.query(File).filter(File.full_path==full_path).one()
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
		return filenum

	def get_neighbors(self, obsnum):
		"""
		get the neighbors given the input obsnum
		input: obsnum
		return: list of two obsnums
		If no neighbor, returns None the list entry

		Todo: test. no close!!
		"""
		s = self.Session()
		OBS = s.query(Observation).filter(Observation.obsnum==obsnum).one()
		try: high = OBS.high_neighbors[0].obsnum
		except(IndexError):high = None
		try: low = OBS.low_neighbors[0].obsnum
		except(IndexError):low=None
		s.close()
		return (low,high)

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

	def get_file_host(self, full_path):
		"""
		todo
		"""
		FILE = self.get_file(full_path)
		return FILE.host

	def set_file_host(self, full_path, host):
		"""
		todo
		"""
		FILE = self.get_file(full_path)
		FILE.host = host
		yay = self.update_file(FILE)
		return yay

	def get_prev_obs(self, obsnum):
		"""
		todo
		"""
		OBS = self.get_obs(obsnum)
		return OBS.prev_obs

	def set_prev_obs(self, obsnum, prev_obs):
		"""
		todo
		"""
		OBS = self.get_obs(obsnum)
		OBS.prev_obs = prev_obs
		yay = self.update_obs(OBS)
		return yay

	def get_next_obs(self, obsnum):
		"""
		todo
		"""
		OBS = self.get_obs(obsnum)
		return OBS.next_obs

	def set_next_obs(self, obsnum, next_obs):
		"""
		todo
		"""
		OBS = self.get_obs(obsnum)
		OBS.next_obs = next_obs
		yay = self.update_obs(OBS)
		return yay

	def get_edge(self, obsnum):
		"""
		todo
		"""
		OBS = self.get_obs(obsnum)
		return OBS.edge

	def set_edge(self, obsnum, edge):
		"""
		todo
		"""
		OBS = self.get_obs(obsnum)
		OBS.edge = edge
		yay = self.update_obs(OBS)
		return yay
