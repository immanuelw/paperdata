from sqlalchemy import Table, Column, String, Integer, ForeignKey, Float, func, Boolean, DateTime, Enum, BigInteger, Numeric, Text
from sqlalchemy import event, DDL, UniqueConstraint, PrimaryKeyConstraint
from sqlalchemy import exc
from sqlalchemy.orm import relationship, backref, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool, QueuePool
import hashlib
import os, sys, logging
try:
    import configparser
except:
    import ConfigParser as configparser

Base = declarative_base()
logger = logging.getLogger('data')

#########
#
#   Useful helper functions
#
#########

str2pol = { 'I' :  1,   # Stokes Paremeters
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

def jdpol2obsnum(jd,pol,djd):
    """
    input: julian date float, pol string. and length of obs in fraction of julian date
    output: a unique index
    """
    dublinjd = jd - 2415020  #use Dublin Julian Date
    obsint = int(dublinjd / djd)  #divide up by length of obs
    try:
        import aipy as a
        polnum = a.miriad.str2pol[pol] + 10
    except:
        polnum = str2pol[pol]+10
    assert(obsint < 2 ** 31)
    return int(obsint + polnum * (2 ** 32))

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
    BLOCKSIZE = 65536
    hasher = hashlib.md5()
    try:
        afile = open(fname, 'rb')
    except(IOError):
        afile = open('{fname}/visdata'.format(fname=fname), 'rb')
    buf = afile.read(BLOCKSIZE)
    while len(buf) >0:
        hasher.update(buf)
        buf = afile.read(BLOCKSIZE)
    return hasher.hexdigest()

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
    #lst = Column(Numeric(3,1))
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
    #                   'lst':self.lst,
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
#   __tablename__ = 'rtp_file'
#   host = Column(String(100), nullable=False)
#   path = Column(String(100), nullable=False) #directory
#   filename = Column(String(100), nullable=False) #zen.*.*.uv/uvcRRE/uvcRREzx...
#   filetype = Column(String(20), nullable=False) #uv, uvcRRE, etc.
#   full_path = Column(String(200), primary_key=True)
#   obsnum = Column(BigInteger, ForeignKey('rtp_observation.obsnum'))
#   filesize = Column(Numeric(7,2))
#   md5sum = Column(String(32))
#   transferred = Column(Boolean)
#   julian_day = Column(Integer)
#   new_host = Column(String(100))
#   new_path = Column(String(100))
#   timestamp = Column(BigInteger)
#   observation = relationship(Rtp_Observation, backref=backref('files', uselist=True))

#   def to_json(self):
#       self.rtp_file_data = {'host':self.host,
#                           'path':self.path,
#                           'filename':self.filename,
#                           'filetype':self.filetype,
#                           'full_path':self.full_path,
#                           'obsnum':self.obsnum,
#                           'filesize':self.filesize,
#                           'md5sum':self.md5sum,
#                           'transferred':self.transferred,
#                           'julian_day':self.julian_day,
#                           'new_host':self.new_host,
#                           'new_path':self.new_path,
#                           'timestamp':self.timestamp}
#       return self.rtp_data

#class Rtp_Observation(Base):
#   __tablename__ = 'rtp_observation'
#   obsnum = Column(BigInteger, primary_key=True)
#   julian_date = Column(Numeric(12,5))
#   polarization = Column(String(4))
#   julian_day = Column(Integer)
#   era = Column(Integer)
#   length = Column(Numeric(6,5)) #length of rtp_observation in fraction of a day
#   prev_obs = Column(BigInteger, unique=True)
#   next_obs = Column(BigInteger, unique=True)
#   timestamp = Column(BigInteger)

#   def to_json(self):
#       self.rtp_obs_data = {'obsnum':self.obsnum,
#                           'julian_date':self.julian_date,
#                           'polarization':self.polarization,
#                           'julian_day':self.julian_day,
#                           'era':self.era,
#                           'length':self.length,
#                           'prev_obs':self.prev_obs, 
#                           'next_obs':self.next_obs,
#                           'timestamp':self.timestamp}
#       return self.rtp_obs_data

#class Rtp_Log(Base):
#   __tablename__ = 'rtp_log'
#   __table_args__ = (PrimaryKeyConstraint('action', 'identifier', 'timestamp', name='action_time'),)
#   action = Column(String(100), nullable=False)
#   table = Column(String(100))
#   identifier = Column(String(200)) #the primary key that is used in other tables of the object being acted on
#   timestamp = Column(BigInteger)
#
#   def to_json(self):
#       self.rtp_log_data = {'action':self.action,
#                           'table':self.table,
#                           'identifier':self.identifier,
#                           'timestamp':self.timestamp}
#       return rtp_log_data


class DataBaseInterface(object):
    def __init__(self, configfile='~/paper.cfg', test=False):
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
        if test:
            self.engine = create_engine('sqlite:///',
                                        connect_args={'check_same_thread':False},
                                        poolclass=StaticPool)
            self.create_db()
        else:
            try:
                connect_string = 'mysql://{username}:{password}@{hostip}:{port}/{dbname}'
                self.engine = create_engine(connect_string.format(**self.dbinfo), pool_size=20, max_overflow=40)
            except:
                connect_string = 'mysql+mysqldb://{username}:{password}@{hostip}:{port}/{dbname}'
                self.engine = create_engine(connect_string.format(**self.dbinfo), pool_size=20, max_overflow=40)

        self.Session = sessionmaker(bind=self.engine)

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

    def get_entry(self, TABLE, unique_value):
        """
        retrieves any object.
        Errors if there are more than one of the same object in the db. This is bad and should
        never happen

        todo:test
        """
        s = self.Session()
        table = getattr(sys.modules[__name__], TABLE.title())
        try:
            ENTRY = s.query(table).get(unique_value)
        except:
            return None
        s.close()
        return ENTRY

    def update_entry(self, ENTRY):
        """
        updates any object field
        ***NEED TO TEST
        """
        s = self.Session()
        s.add(ENTRY)
        s.commit()
        s.close()
        return True

    def set_entry(self, ENTRY, field, new_value):
        """
        sets the value of any entry
        input: ENTRY object, field to be changed, new value
        """
        setattr(ENTRY, field, new_value)
        yay = self.update_entry(ENTRY)
        return yay

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
