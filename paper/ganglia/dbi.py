'''
paper.ganglia.dbi

author | Immanuel Washington

Classes
-------
Filesystem | sqlalchemy table
Monitor | sqlalchemy table
Ram | sqlalchemy table
Iostat | sqlalchemy table
Cpu | sqlalchemy table
DataBaseInterface | interface to ganglia database
'''
from sqlalchemy import Table, Column, String, Integer, ForeignKey, Float, func, Boolean, DateTime, Enum, BigInteger, Numeric, Text
from sqlalchemy import event, DDL
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base
import datetime
import logging
import paper as ppdata

Base = declarative_base()
logger = logging.getLogger('paper.ganglia')

#############
#
#   The basic definition of our database
#
#############

class Filesystem(Base, ppdata.DictFix):
    __tablename__ = 'Filesystem'
    host = Column(String(100), doc='host of system that is being monitored')
    system = Column(String(100), doc='segment of filesystem') #/data4
    total_space = Column(BigInteger, doc='total space in system in bytes')
    used_space = Column(BigInteger, doc='used space in system in bytes')
    free_space = Column(BigInteger, doc='free space in system in bytes')
    percent_space = Column(Numeric(4,1), doc='perecent of used space in system')
    filesystem_id = Column(String(36), primary_key=True, doc='system id')
    timestamp = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, doc='Time entry was last updated')

class Monitor(Base, ppdata.DictFix):
    __tablename__ = 'Monitor'
    host = Column(String(100), doc='node that uv file is being comrpessed on')
    base_path = Column(String(100), doc='directory that file is located in')
    filename = Column(String(100), doc='name of uv file being compressed')
    source = Column(String(200), doc='combination of host, path, and filename')
    status = Column(String(100), doc='state of compression file is currently on')
    full_stats = Column(String(200), primary_key=True, doc='unique id of host, full path, and status')
    del_time = Column(BigInteger, doc='time taken to finish step -- status transition')
    time_start = Column(DateTime, doc='time process started')
    time_end = Column(DateTime, doc='time process ended')
    timestamp = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, doc='Time entry was last updated')

class Ram(Base, ppdata.DictFix):
    __tablename__ = 'Ram'
    host = Column(String(100), doc='host of system that is being monitored')
    total = Column(BigInteger, doc='total ram')
    used = Column(BigInteger, doc='used ram')
    free = Column(BigInteger, doc='free ram')
    shared = Column(BigInteger, doc='shared ram')
    buffers = Column(BigInteger, doc='buffers')
    cached = Column(BigInteger, doc='cached ram')
    bc_used = Column(BigInteger)
    bc_free = Column(BigInteger)
    swap_total = Column(BigInteger)
    swap_used = Column(BigInteger)
    swap_free = Column(BigInteger)
    ram_id = Column(String(36), primary_key=True, doc='ram id')
    timestamp = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, doc='Time entry was last updated')

class Iostat(Base, ppdata.DictFix):
    __tablename__ = 'Iostat'
    host = Column(String(100), doc='host of system that is being monitored')
    device = Column(String(100))
    tps = Column(Numeric(7,2))
    read_s = Column(Numeric(7,2), doc='reads per second')
    write_s = Column(Numeric(7,2), doc='writes per second')
    bl_reads = Column(BigInteger, doc='block reads')
    bl_writes = Column(BigInteger, doc='block writes')
    iostat_id = Column(String(36), primary_key=True, doc='iostat id')
    timestamp = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, doc='Time entry was last updated')

class Cpu(Base, ppdata.DictFix):
    __tablename__ = 'Cpu'
    host = Column(String(100), doc='host of system that is being monitored')
    cpu = Column(Integer, doc='number of cpu/processor being monitored')
    user_perc = Column(Numeric(5,2), doc='percent of cpu being used by user')
    sys_perc = Column(Numeric(5,2), doc='percent of cpu being used by system')
    iowait_perc = Column(Numeric(5,2), doc='percent of cpu waiting')
    idle_perc = Column(Numeric(5,2), doc='percent of cpu that is idle')
    intr_s = Column(Integer, doc='instructions (per second)?')
    cpu_id = Column(String(36), primary_key=True, doc='cpu id')
    timestamp = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, doc='Time entry was last updated')

class DataBaseInterface(ppdata.DataBaseInterface):
    '''
    Database Interface

    Methods
    -------
    create_db | creates all defined tables
    drop_db | drops all tables from database
    '''
    def __init__(self, Base=Base, configfile=ppdata.osj(ppdata.root_dir, 'config', 'ganglia.cfg')):
        '''
        Unique Interface for the ganglia database

        Parameters
        ----------
        Base | object declarative database base
        configfile | Optional[str]: ganglia database configuration file --defaults to ~/ganglia.cfg
        '''
        super(DataBaseInterface, self).__init__(Base=Base, configfile=configfile)

    def create_db(self):
        '''
        creates the tables in the database.
        '''
        self.Base.metadata.bind = self.engine
        table = Monitor.__table__
        insert_update_trigger = DDL('''CREATE TRIGGER insert_update_trigger \
                                        after INSERT or UPDATE on Monitor \
                                        FOR EACH ROW \
                                        SET NEW.source = concat(NEW.host, ':', NEW.base_path, '/', NEW.filename)''')
        event.listen(table, 'after_create', insert_update_trigger)
        insert_update_trigger_2 = DDL('''CREATE TRIGGER insert_update_trigger_2 \
                                        after INSERT or UPDATE on Monitor \
                                        FOR EACH ROW \
                                        SET NEW.full_stats = concat(NEW.source, '&', NEW.status)''')
        event.listen(table, 'after_create', insert_update_trigger_2)
        self.Base.metadata.create_all()

    def drop_db(self):
        '''
        drops tables in the database

        Parameters
        ----------
        Base | object: Base database object
        '''
        super(DataBaseInterface, self).drop_db()
