import os
import sys
import paramiko
import logging
import subprocess
import decimal
from contextlib import contextmanager
from sqlalchemy import exc
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
try:
	import configparser
except:
	import ConfigParser as configparser

def decimal_default(obj):
	'''
	fixes decimal issue with json module

	Args:
		obj (object)

	Returns:
		object: float version of decimal object
	'''
	if isinstance(obj, decimal.Decimal):
		return float(obj)

def rsync_copy(source, destination):
	'''
	uses rsync to copy files and make sure they have not changed by using md5 (c option)

	Args:
		source (str): source file path
		destination (str): destination path
	'''
	subprocess.check_output(['rsync', '-ac', source, destination])

	return None

@contextmanager
def ssh_scope(host, username=None):
	'''
	creates a ssh scope
	can use 'with'
	SSH/SFTP connection to remote host

	Args:
		host (str): remote host
		username (str): username --defaults to None

	Returns:
		object: ssh object to be used to run commands to remote host
	'''
	ssh = paramiko.SSHClient()
	ssh.load_system_host_keys()
	ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	key_filename = os.path.expanduser('~/.ssh/id_rsa')
	try:
		ssh.connect(host, username=username, key_filename=key_filename)
		yield ssh
	except:
		try:
			ssh.connect(host, key_filename=key_filename)
			yield ssh
		except:
			return None
		finally:
			ssh.close()
	finally:
		ssh.close()

logger = logging.getLogger('paper')
Base = declarative_base()

class DictFix(object):
	'''
	superclass for all SQLAlchemy Table objects
	allows access to object row dictionary
	'''
	def to_dict(self):
		'''
		convert object to dict

		Returns:
			dict: table attributes
		'''
		try:
			new_dict = {}
			for column in self.__table__.columns:
				new_dict[column.name] = getattr(self, column.name)
			return new_dict
		except(exc.InvalidRequestError):
			return None

class DataBaseInterface(object):
	def __init__(self, configfile='~/paperdata.cfg'):
		'''
		Connect to the database and initiate a session creator.
		superclass of DBI for paperdata, paperdev, and ganglia databases

		Args:
			configfile (Optional[str]): configuration file --defaults to ~/paperdata.cfg
		'''
		if not configfile is None:
			config = configparser.ConfigParser()
			configfile = os.path.expanduser(configfile)
			if os.path.exists(configfile):
				logger.info(' '.join(('loading file', configfile)))
				config.read(configfile)
				try:
					self.dbinfo = config._sections['dbinfo']
				except:
					self.dbinfo = config['dbinfo']
				try:
					self.dbinfo['password'] = self.dbinfo['password'].decode('string-escape')
				except:
					try:
						self.dbinfo['password'] = bytes(self.dbinfo['password'], 'ascii').decode('unicode_escape')
					except:
						self.dbinfo['password'] = self.dbinfo['password']
			else:
				logging.info(' '.join((configfile, 'Not Found')))
		try:
			connect_string = 'mysql://{username}:{password}@{hostip}:{port}/{dbname}'
			self.engine = create_engine(connect_string.format(**self.dbinfo), pool_size=20, max_overflow=40)
		except:
			connect_string = 'mysql+mysqldb://{username}:{password}@{hostip}:{port}/{dbname}'
			self.engine = create_engine(connect_string.format(**self.dbinfo), pool_size=20, max_overflow=40)

		self.Session = sessionmaker(bind=self.engine)

	@contextmanager
	def session_scope(self):
		'''
		creates a session scope
		can use 'with'

		Returns:
			object: session scope to be used to access database with 'with'
		'''
		session = self.Session()
		try:
			yield session
			session.commit()
		except:
			session.rollback()
			raise
		finally:
			session.close()

	def drop_db(self, Base):
		'''
		drops the tables in the database.

		Args:
			Base (object): base object for database
		'''
		Base.metadata.bind = self.engine
		Base.metadata.drop_all()

	def create_table(Table):
		'''
		creates a table in the database.

		Args:
			Table (object): table object
		'''
		Table.__table__.create(bind=self.engine)

	def get_entry(self, TABLE, unique_value, s=None, open_sess=False):
		'''
		retrieves any object.
		Errors if there are more than one of the same object in the db. This is bad and should
		never happen

		Args:
			TABLE (str): table name
			unique_value (int/float/str): primary key value of row
			s (Optional[object]): session object --defaults to None
			open_sess (Optional[bool]): is there a session open --defaults to False

		Returns:
			object: table object
		'''
		if s is None:
			s = self.Session()
			open_sess = True
		table = getattr(sys.modules[__name__], TABLE.title())
		try:
			ENTRY = s.query(table).get(unique_value)
		except:
			return None
		if open_sess:
			s.close()

		return ENTRY

	def set_entry(self, ENTRY, field, new_value, s=None, open_sess=False):
		'''
		sets the value of any entry

		Args:
			ENTRY (object): entry object
			field (str): field to be changed
			new_value (int/float/str): value to change field in entry to
			s (Optional[object]): session object --defaults to None
			open_sess (Optional[bool]): is there a session open --defaults to False
		'''
		if s is None:
			s = self.Session()
			open_sess = True
		setattr(ENTRY, field, new_value)
		self.add_entry(s, ENTRY)
		if open_sess:
			s.close()

		return None

	def add_entry(self, ENTRY, s=None, open_sess=False):
		'''
		adds entry to database and commits
		does not add if duplicate found

		Args:
		Args:
			ENTRY (object): entry object
			s (Optional[object]): session object --defaults to None
			open_sess (Optional[bool]): is there a session open --defaults to False
		'''
		if s is None:
			s = self.Session()
			open_sess = True
		try:
			s.add(ENTRY)
			s.commit()
		except (exc.IntegrityError):
			s.rollback()
			print('Duplicate entry found ... skipping entry')
		if open_sess:
			s.close()

		return None
