'''
paper

author | Immanuel Washington

Functions
---------
file_to_jd | pulls julian date from filename
file_to_pol | pulls polarization from filename
decimal_default | json fix for decimal types
json_data | dumps objects into json file
rsync_copy | pythonic rsync
ssh_scope | ssh connection

Classes
-------
DictFix | adds dictionary to sqlalchemy objects
DataBaseInterface | interface to database through sqlalchemy

Modules
-------
convert | time conversions
memory | memory checking
schema | schema table creation

Subpackages
-----------
calibrate | calibration and conversion uv files into timestream hdf5 files
data | (main subpackage) adding, updating, moving, and deleting files, observations, and entries in the paperdata database
dev | dev version of data subpackage, for testing new features
distiller | access to paperdistiller database and its features
ganglia | logging of and access to host information
site | websites built on flask for accessing the paperdata database
'''
from __future__ import print_function
import os
import sys
import decimal
import json
import logging
import paramiko
import re
import subprocess
from contextlib import contextmanager
from sqlalchemy import exc
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
try:
    import configparser
except ImportError as e:
    import ConfigParser as configparser

root_dir = os.path.realpath(os.path.join(os.path.dirname(__file__), '..'))
osj = os.path.join

def file_to_jd(path):
    '''
    pulls julian date from filename using regex

    Parameters
    ----------
    path | str: path of file

    Returns
    -------
    str: julian date

    >>> file_to_jd('/home/immwa/test_data/zen.2456617.17386.xx.uvcRRE')
    2456617.17386
    '''
    try:
        jd = round(float(re.findall(r'\d+\.\d+', path)[0]), 5)
    except IndexError as e:
        jd = None

    return jd

def file_to_pol(path):
    '''
    pulls polarization from filename using regex

    Parameters
    ----------
    path | str: path of file

    Returns
    -------
    str: polarization

    >>> file_to_pol('/home/immwa/test_data/zen.2456617.17386.xx.uvcRRE')
    'xx'
    '''
    try:
        polarization = re.findall(r'\.(.{2})\.', path)[0]
        #polarization = re.findall(r'\.([xy][xy])\.', path)[0]
    except IndexError as e:
        polarization = 'all'

    return polarization

def decimal_default(obj):
    '''
    fixes decimal issue with json module

    Parameters
    ----------
    obj (object)

    Returns
    -------
    object: float version of decimal object
    '''
    if isinstance(obj, decimal.Decimal):
        return float(obj)

def json_data(backup_path, dump_objects):
    '''
    dumps list of objects into a json file

    Parameters
    ----------
    backup_path | str: filename
    dump_objects | list[object]: database objects query
    '''
    with open(backup_path, 'w') as bkup:
        data = [ser_data.to_dict() for ser_data in dump_objects.all()]
        json.dump(data, bkup, sort_keys=True, indent=1, default=decimal_default)

def rsync_copy(source, destination):
    '''
    uses rsync to copy files
    make sure they have not changed by using md5 (c option)

    Parameters
    ----------
    source | str: source file path
    destination | str: destination path
    '''
    subprocess.check_output(['rsync', '-ac', source, destination])

    return None

@contextmanager
def ssh_scope(host, username=None, password=None):
    '''
    creates a ssh scope
    can use 'with'
    SSH/SFTP connection to remote host

    Parameters
    ----------
    host | str: remote host
    username | str: username --defaults to None
    password | str: password --defaults to None

    Returns
    -------
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
            try:
                ssh.connect(host, username=username, password=password, key_filename=key_filename)
                yield ssh
            except:
                raise #'Did not connect through ssh'
    finally:
        ssh.close()

logger = logging.getLogger('paper')
Base = declarative_base()

class DictFix(object):
    '''
    superclass for all SQLAlchemy Table objects
    allows access to object row dictionary

    Methods
    -------
    to_dict | creates python dict of fields from sqlalchemy object
    '''
    def to_dict(self):
        '''
        convert object to dict

        Returns
        -------
        dict: table attributes
        '''
        try:
            new_dict = {column.name: getattr(self, column.name) for column in self.__table__.columns}
            return new_dict
        except(exc.InvalidRequestError):
            return None

class DataBaseInterface(object):
    '''
    Database Interface
    base class used to connect to databases

    Methods
    -------
    session_scope | context manager for session connection to database
    drop_db | drops all tables from database
    create_table | creates individual table in database
    '''
    def __init__(self, Base=Base, configfile=osj(root_dir, 'config/paperdata.cfg')):
        '''
        Connect to the database and make a session creator
        superclass of DBI for paperdata, paperdev, and ganglia databases

        Parameters
        ----------
        Base | object: declarative database base
        configfile | Optional[str]: configuration file --defaults to ~/paperdata.cfg
        '''
        if configfile is not None:
            config = configparser.ConfigParser()
            configfile = os.path.expanduser(configfile)
            if os.path.exists(configfile):
                config.read(configfile)
                logger.info(' '.join(('loading file', configfile)))
                try:
                    self.dbinfo = config._sections['dbinfo']
                except AttributeError as e:
                    print(e, ':', 'Config file has wrong format')
                    raise 
                try:
                    self.dbinfo['password'] = self.dbinfo['password'].decode('string-escape')
                except AttributeError as e:
                    self.dbinfo['password'] = bytes(self.dbinfo['password'], 'ascii').decode('unicode_escape')
            else:
                logging.info(' '.join((configfile, 'Not Found')))
                print('Config file does not exist')
                raise FileNotFoundError
            try:
                connect_string = 'mysql://{username}:{password}@{hostip}:{port}/{dbname}'
                self.engine = create_engine(connect_string.format(**self.dbinfo), pool_size=20, max_overflow=40)
            except:
                connect_string = 'mysql+mysqldb://{username}:{password}@{hostip}:{port}/{dbname}'
                self.engine = create_engine(connect_string.format(**self.dbinfo), pool_size=20, max_overflow=40)

            self.Session = sessionmaker(bind=self.engine)
            self.Base = Base

    @contextmanager
    def session_scope(self):
        '''
        creates a session scope
        can use 'with'

        Returns
        -------
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

    def drop_db(self):
        '''
        drops the tables in the database.

        Parameters
        ----------
        Base | object: base object for database
        '''
        self.Base.metadata.bind = self.engine
        self.Base.metadata.drop_all()
