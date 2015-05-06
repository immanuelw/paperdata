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

class File(Base):
	__tablename__ = 'file'
	filenum = Column(Integer, primary_key=True)
	host = Column(String(100))
	path = Column(String(100)) #directory
	filename = Column(String(100)) #zen.*.*.uv/uvcRRE/uvcRREzx...
	filetype = Column(String(20)) #uv, uvcRRE, etc.
	###
	#UniqueConstraint('host', 'path', 'filename', name='full_path')
	###
	obsnum = Column(BigInteger, ForeignKey('observation.obsnum'))
	filesize = Column(Numeric(7,2))
	md5sum = Column(Integer)
	tape_index = Column(String(100))
	###
	#full_path = Column(String(200))
	#use trigger!!!
	###
	time_start = Column(Numeric(12,5))
	time_end = Column(Numeric(12,5))
	delta_time = Column(Numeric(12,5))
	prev_obs = Column(BigInteger, ForeignKey('observation.obsnum'))
	next_obs = Column(BigInteger, ForeignKey('observation.obsnum'))
	### maybe unnecessary fields
	status = Column(String(20))
	calibration_path = Column(String(100))
	#history?
	compressed = Column(Boolean)
	edge = Column(Boolean)
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
			self.createdb()
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

	def get_file(self, filename):
		"""
		retrieves an file object.
		Errors if there are more than one of the same file in the db. This is bad and should
		never happen

		todo:test
		"""
		s = self.Session()
		OBS = s.query(File).filter(File.filename==filename).one()
		s.close()
		return FILE

	def createdb(self):
		"""
		creates the tables in the database.
		"""
		Base.metadata.bind = self.engine
		Base.metadata.create_all()

	def add_observation(self, julian_date, polarization, host, path, filename, filetype, length=10/60./24):
		"""
		create a new observation entry.
		returns: obsnum  (see jdpol2obsnum)
		Note: does not link up neighbors!
		"""
		OBS = Observation(julian_date=julian_date, polarization=polarization, length=length)
		s = self.Session()
		s.add(OBS)
		s.commit()
		obsnum = OBS.obsnum
		s.close()
		self.add_file(obsnum, host, filename, filetype)#todo test.
		sys.stdout.flush()
		return obsnum

	def add_file(self, obsnum, host, path, filename, filetype):
		"""
		Add a file to the database and associate it with an observation.
		"""
		FILE = File(filename=filename,host=host,path=path,filetype=filetype)
		#get the observation corresponding to this file
		s = self.Session()
		OBS = s.query(Observation).filter(Observation.obsnum==obsnum).one()
		FILE.observation = OBS  #associate the file with an observation
		s.add(FILE)
		s.commit()
		filenum = FILE.filenum #we gotta grab this before we close the session.
		s.close() #close the session
		return filenum

	def add_observations(self, obslist, status='UV_POT'):
		"""
		Add a whole set of observations.
		Handles linking neighboring observations.

		input: list of dicts where the dict has the parameters needed as inputs to add_observation:
		julian_date
		pol (anything in a.miriad.str2pol)
		host
		file
		length (in fractional jd)
		neighbor_high (julian_date)
		neighbor_low  (julian_date)

		What it does:
		adds observations with status NEW
		Links neighboring observations in the database
		"""
		neighbors = {}
		for obs in obslist:
			obsnum = self.add_observation(obs['julian_date'],obs['pol'],
							obs['filename'],obs['host'],
							length=obs['length'],status='NEW')
			neighbors[obsnum] = (obs.get('neighbor_low',None),obs.get('neighbor_high',None))
		s = self.Session()
		for middleobsnum in neighbors:
			OBS = s.query(Observation).filter(Observation.obsnum==middleobsnum).one()
			if not neighbors[middleobsnum][0] is None:
				L = s.query(Observation).filter(
						Observation.julian_date==neighbors[middleobsnum][0],
						Observation.pol == OBS.pol).one()
				OBS.low_neighbors = [L]
			if not neighbors[middleobsnum][1] is None:
				H = s.query(Observation).filter(
						Observation.julian_date==neighbors[middleobsnum][1],
						Observation.pol == OBS.pol).one()
				OBS.high_neighbors = [H]
				sys.stdout.flush()
			OBS.status = status
			s.add(OBS)
			s.commit()
		s.close()
		return neighbors.keys()

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

	def get_file_path(self, filename):
		"""
		todo
		"""
		FILE = self.get_file(filename)
		return FILE.path

	def set_file_path(self, filename, path):
		"""
		todo
		"""
		FILE = self.get_file(filename)
		FILE.path = path
		yay = self.update_file(FILE)
		return yay

	def get_file_host(self, filename):
		"""
		todo
		"""
		FILE = self.get_file(filename)
		return FILE.host

	def set_file_host(self, filename, host):
		"""
		todo
		"""
		FILE = self.get_file(filename)
		FILE.host = host
		yay = self.update_file(FILE)
		return yay

	def get_file_md5sum(self, filename):
		"""
		todo
		"""
		FILE = self.get_file(filename)
		return FILE.md5sum

	def set_file_md5sum(self, filename, md5sum):
		"""
		todo
		"""
		FILE = self.get_file(filename)
		FILE.md5sum = md5sum
		yay = self.update_file(FILE)
		return yay

	def get_file_prev_obs(self, filename):
		"""
		todo
		"""
		FILE = self.get_file(filename)
		return FILE.prev_obs

	def set_file_prev_obs(self, filename, prev_obs):
		"""
		todo
		"""
		FILE = self.get_file(filename)
		FILE.prev_obs = prev_obs
		yay = self.update_file(FILE)
		return yay

	def get_file_next_obs(self, filename):
		"""
		todo
		"""
		FILE = self.get_file(filename)
		return FILE.next_obs

	def set_file_next_obs(self, filename, next_obs):
		"""
		todo
		"""
		FILE = self.get_file(filename)
		FILE.next_obs = next_obs
		yay = self.update_file(FILE)
		return yay

	def get_input_file(self,obsnum):
		"""
		input:observation number
		return: host,path (the host and path of the initial data set on the pot)

		todo:test
		"""
		s = self.Session()
		OBS = s.query(Observation).filter(Observation.obsnum==obsnum).one()
		POTFILE = s.query(File).filter(
			File.observation==OBS,
			#File.host.like('%pot%'), # XXX temporarily commenting this out.  need a better solution for finding original file
			File.filename.like('%uv')).one()
		host = POTFILE.host
		path = os.path.dirname(POTFILE.filename)
		file = os.path.basename(POTFILE.filename)
		return host,path,file

	def get_output_location(self,obsnum):
		"""
		input: observation number
		return: host,path
		TODO: test
		"""
		#right now we're pointing the output at the input location (nominally whatever pot
		#	the data came from
		host,path,inputfile = self.get_input_file(obsnum)
		return host,path

