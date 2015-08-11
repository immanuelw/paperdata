from sqlalchemy import Table, Column, String, Integer, ForeignKey, Float, func, Boolean, DateTime, Enum, BigInteger, Numeric, Text
from sqlalchemy import event, DDL, UniqueConstraint
from sqlalchemy import exc
from sqlalchemy.orm import relationship, backref, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool, QueuePool
import aipy as a, os, numpy as n, sys, logging
import configparser
import hashlib
#Based on example here: http://www.pythoncentral.io/overview-sqlalchemys-expression-language-orm-queries/
Base = declarative_base()
logger = logging.getLogger('paperdev_dbi')

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
	key_filename = os.path.expanduser('~/.ssh/id_rsa')
	try:
		ssh.connect(host, username=username, key_filename=key_filename)
	except:
		try:
			ssh.connect(host, key_filename=key_filename)
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
	timestamp = Column(BigInteger)

	def to_json(self):
		self.obs_data = {'obsnum':self.obsnum,
						'julian_date':self.julian_date,
						'polarization':self.polarization,
						'julian_day':self.julian_day,
						'era':self.era,
						'era_type':self.era_type,
						'length':self.length,
						'time_start':self.time_start,
						'time_end':self.time_end,
						'delta_time':self.delta_time,
						'prev_obs':self.prev_obs, 
						'next_obs':self.next_obs,
						'edge':self.edge,
						'timestamp':self.timestamp}
		return self.obs_data

class File(Base):
	__tablename__ = 'file'
	#filenum = Column(Integer, primary_key=True)
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
	#calibration_path = Column(String(100))
	#history?
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
						'obsnum':self.obsnum,
						'filesize':self.filesize,
						'md5sum':self.md5sum,
						'tape_index':self.tape_index,
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

class DataBaseInterface(object):
	def __init__(self,configfile='~/paperdev.cfg',test=False):
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
		try:
			OBS = s.query(Observation).filter(Observation.obsnum==obsnum).one()
		except:
			return None
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
		try:
			FILE = s.query(File).filter(File.full_path==full_path).one()
		except:
			return None
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
		try:
			FEED = s.query(Feed).filter(Feed.full_path==full_path).one()
		except:
			return None
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
		insert_update_trigger = DDL('''CREATE TRIGGER insert_update_trigger \
										after INSERT or UPDATE on File \
										FOR EACH ROW \
										SET NEW.full_path = concat(NEW.host, ':', NEW.path, '/', NEW.filename)''')
		event.listen(File.__table__, 'after_create', insert_update_trigger)
		Base.metadata.create_all()

	def create_table(Table):
		"""
		creates a table in the database.
		"""
		Table.__table__.create(bind=self.engine)

	def drop_db(self):
		"""
		drops the tables in the database.
		"""
		Base.metadata.bind = self.engine
		Base.metadata.drop_all()

	def add_entry(self, ENTRY):
		s = self.Session()
		try:
			s.add(ENTRY)
			s.commit()
		except (exc.IntegrityError):
			s.rollback()
			s.close()
			print('Duplicate entry found ... skipping entry')
			return None
		s.close()
		return None

	def add_observation(self, obsnum, julian_date, polarization, julian_day, era, era_type, length, time_start, time_end, delta_time,
						prev_obs, next_obs, edge, timestamp):
		"""
		create a new observation entry.
		returns: obsnum  (see jdpol2obsnum)
		Note: does not link up neighbors!
		"""
		OBS = Observation(obsnum=obsnum, julian_date=julian_date, polarization=polarization, julian_day=julian_day, era=era, era_type=era_type,
							length=length, time_start=time_start, time_end=time_end, delta_time=delta_time,	prev_obs=prev_obs,
							next_obs=next_obs, edge=edge, timestamp=timestamp)
		self.add_entry(OBS)
		obsnum = OBS.obsnum
		sys.stdout.flush()
		return obsnum

	def add_file(self, host, path, filename, filetype, full_path, obsnum, filesize, md5sum, tape_index, write_to_tape, delete_file, timestamp):
		"""
		Add a file to the database and associate it with an observation.
		"""
		FILE = File(host=host, path=path, filename=filename, filetype=filetype, full_path=full_path, obsnum=obsnum, filesize=filesize,
					md5sum=md5sum, tape_index=tape_index, write_to_tape=write_to_tape, delete_file=delete_file, timestamp=timestamp)
		#get the observation corresponding to this file
		OBS = s.query(Observation).filter(Observation.obsnum==obsnum).one()
		FILE.observation = OBS  #associate the file with an observation
		self.add_entry(FILE)
		return None

	def add_feed(self, host, path, filename, ready_to_move, moved_to_distill, timestamp):
		"""
		Add a feed to the database
		"""
		FEED = Feed(host=host, path=path, filename=filename, ready_to_move=ready_to_move, moved_to_distill=moved_to_distill, timestamp=timestamp)
		self.add_entry(FILE)
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

	def get_file_md5(self, full_path):
		"""
		todo
		"""
		FILE = self.get_file(full_path)
		return FILE.md5sum

	def set_file_md5(self, full_path, md5):
		"""
		todo
		"""
		FILE = self.get_file(full_path)
		FILE.md5sum = md5
		yay = self.update_file(FILE)
		return yay

	def get_file_write(self, full_path):
		"""
		todo
		"""
		FILE = self.get_file(full_path)
		return FILE.write_to_tape

	def set_file_write(self, full_path, write_to_tape):
		"""
		todo
		"""
		FILE = self.get_file(full_path)
		FILE.write_to_tape = write_to_tape
		yay = self.update_file(FILE)
		return yay

	def get_file_delete(self, full_path):
		"""
		todo
		"""
		FILE = self.get_file(full_path)
		return FILE.delete_file

	def set_file_delete(self, full_path, delete_file):
		"""
		todo
		"""
		FILE = self.get_file(full_path)
		FILE.delete_file = delete_file
		yay = self.update_file(FILE)
		return yay

	def get_file_time(self, full_path):
		"""
		todo
		"""
		FILE = self.get_file(full_path)
		return FILE.timestamp

	def set_file_time(self, full_path, timestamp):
		"""
		todo
		"""
		FILE = self.get_file(full_path)
		FILE.timestamp = timestamp
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

	def get_obs_time(self, full_path):
		"""
		todo
		"""
		OBS = self.get_obs(full_path)
		return OBS.timestamp

	def set_obs_time(self, full_path, timestamp):
		"""
		todo
		"""
		OBS = self.get_obs(full_path)
		OBS.timestamp = timestamp
		yay = self.update_obs(OBS)
		return yay

	def get_feed_path(self, full_path):
		"""
		todo
		"""
		FEED = self.get_feed(full_path)
		return FEED.path

	def set_feed_path(self, full_path, path):
		"""
		todo
		"""
		FEED = self.get_feed(full_path)
		FEED.path = path
		yay = self.update_feed(FEED)
		return yay

	def get_feed_host(self, full_path):
		"""
		todo
		"""
		FEED = self.get_feed(full_path)
		return FEED.host

	def set_feed_host(self, full_path, host):
		"""
		todo
		"""
		FEED = self.get_feed(full_path)
		FEED.host = host
		yay = self.update_feed(FEED)
		return yay

	def get_feed_move(self, full_path):
		"""
		todo
		"""
		FEED = self.get_feed(full_path)
		return FEED.moved_to_distill

	def set_feed_move(self, full_path, move):
		"""
		todo
		"""
		FEED = self.get_feed(full_path)
		FEED.moved_to_distill = move
		yay = self.update_feed(FEED)
		return yay

	def get_feed_ready(self, full_path):
		"""
		todo
		"""
		FEED = self.get_feed(full_path)
		return FEED.ready_to_move

	def set_feed_ready(self, full_path, ready):
		"""
		todo
		"""
		FEED = self.get_feed(full_path)
		FEED.ready_to_move = ready
		yay = self.update_feed(FEED)
		return yay

	def get_feed_time(self, full_path):
		"""
		todo
		"""
		FEED = self.get_feed(full_path)
		return FEED.timestamp

	def set_feed_time(self, full_path, timestamp):
		"""
		todo
		"""
		FEED = self.get_feed(full_path)
		FEED.timestamp = timestamp
		yay = self.update_feed(FEED)
		return yay
