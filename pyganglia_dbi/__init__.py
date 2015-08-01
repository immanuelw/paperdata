from sqlalchemy import Table, Column, String, Integer, ForeignKey, Float, func, Boolean, DateTime, Enum, BigInteger, Numeric, Text
from sqlalchemy import event, DDL, UniqueConstraint, PrimaryKeyConstraint
from sqlalchemy.orm import relationship, backref, sessionmaker
from sqlalchemy import exc
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool, QueuePool
import os, numpy as n, sys, logging
import configparser
#Based on example here: http://www.pythoncentral.io/overview-sqlalchemys-expression-language-orm-queries/
Base = declarative_base()
logger = logging.getLogger('pyganglia_dbi')

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
		ssh.connect(host, username=username, key_filename='~/.ssh/id_rsa')
	except:
		try:
			ssh.connect(host, key_filename='~/.ssh/id_rsa')
		except:
			return None

	return ssh

#############
#
#   The basic definition of our database
#
#############

class Filesystem(Base):
	__tablename__ = 'filesystem'
	__table_args__ = (PrimaryKeyConstraint('host', 'system', 'time_date', name='host_system_time'),)
	host = Column(String(100)) #folio
	system = Column(String(100)) #/data4
	total_space = Column(BigInteger)
	used_space = Column(BigInteger)
	free_space = Column(BigInteger)
	percent_space = Numeric(4,1)
	time_date = Column(BigInteger) #seconds since 1970

class Monitor(Base):
	__tablename__ = 'monitor'
	host = Column(String(100))
	path = Column(String(100))
	filename = Column(String(100))
	full_path = Column(String(200))
	status = Column(String(100))
	full_stats = Column(String(200), primary_key=True)
	del_time = Column(BigInteger)
	time_start = Column(BigInteger)
	time_end = Column(BigInteger)
	time_date = Column(BigInteger)

class Ram(Base):
	__tablename__ = 'ram'
	__table_args__ = (PrimaryKeyConstraint('host', 'time_date', name='host_time'),)
	host = Column(String(100))
	total = Column(BigInteger)
	used = Column(BigInteger)
	free = Column(BigInteger)
	shared = Column(BigInteger)
	buffers = Column(BigInteger)
	cached = Column(BigInteger)
	bc_used = Column(BigInteger)
	bc_free = Column(BigInteger)
	swap_total = Column(BigInteger)
	swap_used = Column(BigInteger)
	swap_free = Column(BigInteger)
	time_date = Column(BigInteger)

class Iostat(Base):
	__tablename__ = 'iostat'
	__table_args__ = (PrimaryKeyConstraint('host', 'time_date', name='host_time'),)
	host = Column(String(100))
	device = Column(String(100))
	tps = Column(Numeric(7,2))
	read_s = Column(Numeric(7,2))
	write_s = Column(Numeric(7,2))
	bl_reads = Column(BigInteger)
	bl_writes = Column(BigInteger)
	time_date = Column(BigInteger)

class Cpu(Base):
	__tablename__ = 'cpu'
	__table_args__ = (PrimaryKeyConstraint('host', 'time_date', name='host_time'),)
	host = Column(String(100))
	cpu = Column(Integer)
	user_perc = Column(Numeric(5,2))
	sys_perc = Column(Numeric(5,2))
	iowait_perc = Column(Numeric(5,2))
	idle_perc = Column(Numeric(5,2))
	intr_s = Column(Integer)
	time_date = Column(BigInteger)

class DataBaseInterface(object):
	def __init__(self,configfile='~/pyg_still.cfg',test=False):
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

	def get_monitor(self, full_stats):
		"""
		retrieves an monitor object.
		Errors if there are more than one of the same monitor in the db. This is bad and should
		never happen

		todo:test
		"""
		s = self.Session()
		try:
			MONITOR = s.query(Monitor).filter(Monitor.full_stats==full_stats).one()
		except:
			return None
		s.close()
		return MONITOR

	def update_monitor(self, MONITOR):
		"""
		updates monitor object field
		***NEED TO TEST
		"""
		s = self.Session()
		s.add(MONITOR)
		s.commit()
		s.close()
		return True

	def create_db(self):
		"""
		creates the tables in the database.
		"""
		Base.metadata.bind = self.engine
		table = Monitor.__table__
		insert_update_trigger = DDL('''CREATE TRIGGER insert_update_trigger \
										after INSERT or UPDATE on Monitor \
										FOR EACH ROW \
										SET NEW.full_path = concat(NEW.host, ':', NEW.path, '/', NEW.filename)''')
		event.listen(table, 'after_create', insert_update_trigger)
		insert_update_trigger_2 = DDL('''CREATE TRIGGER insert_update_trigger_2 \
										after INSERT or UPDATE on Monitor \
										FOR EACH ROW \
										SET NEW.full_stats = concat(NEW.full_path, '+', NEW.status)''')
		event.listen(table, 'after_create', insert_update_trigger_2)
		Base.metadata.create_all()

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

	def add_filesystem(self, host, system, total_space, used_space, free_space, time_date):
		"""
		create a new filesystem entry.
		"""
		FILESYSTEM = Filesystem(host=host, system=system, total_space=total_space, used_space=used_space, free_space=free_space,
								time_date=time_date)
		self.add_entry(FILESYSTEM)
		return None

	def add_monitor(self, host, path, filename, full_path, status, full_stats, del_time, file_start, file_end, time_date):
		"""
		create a new monitor entry.
		"""
		MONITOR = Monitor(host=host, path=path, filename=filename, full_path=full_path, status=status, full_stats=full_stats,
							del_time=del_time, file_start=file_start, file_end=file_end, time_date=time_date)
		self.add_entry(MONITOR)
		return None

	def add_ram(self, host, total, used, free, shared, buffers, cached, bc_used, bc_free, swap_total, swap_used, swap_free, time_date):
		"""
		create a new ram entry.
		"""
		RAM = Ram(host=host, total=total, used=used, free=free, shared=shared, buffers=buffers, cached=cached, bc_used=bc_used, bc_free=bc_free,
					swap_total=swap_total, swap_used=swap_used, swap_free=swap_free, time_date=time_date)
		self.add_entry(RAM)
		return None

	def add_iostat(self, host, device, tps, read_s, write_s, bl_reads, bl_writes, time_date):
		"""
		create a new iostat entry.
		"""
		IOSTAT = Iostat(host=host, device=device, tps=tps, read_s=read_s, write_s=write_s, bl_reads=bl_reads, bl_writes=bl_writes,
						time_date=time_date)
		self.add_entry(IOSTAT)
		return None

	def add_cpu(self, host, cpu, user_perc, sys_perc, iowait_perc, idle_perc, intr_s, time_date):
		"""
		create a new cpu entry.
		"""
		CPU = Cpu(host=host, cpu=cpu, user_perc=user_perc, sys_perc=sys_perc, iowait_perc=iowait_perc, idle_perc=idle_perc, intr_s=intr_s, time_date=time_date)
		self.add_entry(CPU)
		return None

	def get_monitor_path(self, full_stats):
		"""
		todo
		"""
		MONITOR = self.get_monitor(full_stats)
		return MONITOR.path

	def set_monitor_path(self, full_stats, path):
		"""
		todo
		"""
		MONITOR = self.get_monitor(full_stats)
		MONITOR.path = path
		yay = self.update_monitor(MONITOR)
		return yay
