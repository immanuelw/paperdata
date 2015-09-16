from sqlalchemy import Table, Column, String, Integer, ForeignKey, Float, func, Boolean, DateTime, Enum, BigInteger, Numeric, Text
from sqlalchemy import event, DDL, UniqueConstraint, PrimaryKeyConstraint
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base
import os, sys, logging
import paperdata as ppdata

Base = declarative_base()
logger = logging.getLogger('ganglia')

#############
#
#   The basic definition of our database
#
#############

class Filesystem(Base):
	__tablename__ = 'filesystem'
	__table_args__ = (PrimaryKeyConstraint('host', 'system', 'timestamp', name='host_system_time'),)
	host = Column(String(100)) #folio
	system = Column(String(100)) #/data4
	total_space = Column(BigInteger)
	used_space = Column(BigInteger)
	free_space = Column(BigInteger)
	percent_space = Column(Numeric(4,1))
	timestamp = Column(BigInteger) #seconds since 1970

	def to_json(self):
		self.data_dict = {'host':self.host,
						'system':self.system,
						'total_space':self.total_space,
						'used_space':self.used_space,
						'free_space':self.free_space,
						'percent_space':self.percent_space,
						'timestamp':self.timestamp}
		return self.data_dict

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
	timestamp = Column(BigInteger)

	def to_json(self):
		self.data_dict = {'host':self.host,
						'path':self.path,
						'filename':self.filename,
						'full_path':self.full_path,
						'status':self.status,
						'full_stats':self.full_stats,
						'del_time':self.del_time,
						'time_start':self.time_start,
						'time_end':self.time_end,
						'timestamp':self.timestamp}
		return self.data_dict

class Ram(Base):
	__tablename__ = 'ram'
	__table_args__ = (PrimaryKeyConstraint('host', 'timestamp', name='host_time'),)
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
	timestamp = Column(BigInteger)

	def to_json(self):
		self.data_dict = {'host':self.host,
						'total':self.total,
						'used':self.used,
						'free':self.free,
						'shared':self.shared,
						'buffers':self.buffers,
						'cached':self.cached,
						'bc_used':self.bc_used,
						'bc_free':self.bc_free,
						'swap_total':self.swap_total,
						'swap_used':self.swap_used,
						'swap_free':self.swap_free,
						'timestamp':self.timestamp}
		return self.data_dict

class Iostat(Base):
	__tablename__ = 'iostat'
	__table_args__ = (PrimaryKeyConstraint('host', 'timestamp', name='host_time'),)
	host = Column(String(100))
	device = Column(String(100))
	tps = Column(Numeric(7,2))
	read_s = Column(Numeric(7,2))
	write_s = Column(Numeric(7,2))
	bl_reads = Column(BigInteger)
	bl_writes = Column(BigInteger)
	timestamp = Column(BigInteger)

	def to_json(self):
		self.data_dict = {'host':self.host,
						'device':self.device,
						'tps':self.tps,
						'read_s':self.read_s,
						'write_s':self.write_s,
						'bl_reads':self.bl_reads,
						'bl_writes':self.bl_writes,
						'timestamp':self.timestamp}
		return self.data_dict

class Cpu(Base):
	__tablename__ = 'cpu'
	__table_args__ = (PrimaryKeyConstraint('host', 'timestamp', name='host_time'),)
	host = Column(String(100))
	cpu = Column(Integer)
	user_perc = Column(Numeric(5,2))
	sys_perc = Column(Numeric(5,2))
	iowait_perc = Column(Numeric(5,2))
	idle_perc = Column(Numeric(5,2))
	intr_s = Column(Integer)
	timestamp = Column(BigInteger)

	def to_json(self):
		self.data_dict = {'host':self.host,
						'cpu':self.cpu,
						'user_perc':self.user_perc,
						'sys_perc':self.sys_perc,
						'iowait_perc':self.iowait_perc,
						'idle_perc':self.idle_perc,
						'intr_s':self.intr_s,
						'timestamp':self.timestamp}
		return self.data_dict

class DataBaseInterface(ppdata.DataBaseInterface):
	def __init__(self):
		super(DataBaseInterface, self).__init__(configfile='~/ganglia.cfg', test=False)

	def create_db(self):
		"""
		creates the tables in the database.
		"""
		Base.metadata.bind = self.engine
		table_name = getattr(sys.modules[__name__], 'Monitor')
		table = getattr(table_name, '__table__')
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

	def add_to_table(self, TABLE, entry_dict):
		"""
		create a new entry.
		"""
		table = getattr(sys.modules[__name__], TABLE.title())
		if TABLE in ('filesystem', 'monitor', 'ram', 'iostat', 'cpu'):
			ENTRY = table(**entry_dict)
		self.add_entry(ENTRY)
		return None