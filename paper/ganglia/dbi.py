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
import logging
import paper as ppdata

Base = ppdata.Base
logger = logging.getLogger('paper.ganglia')

#############
#
#   The basic definition of our database
#
#############

class Filesystem(Base, ppdata.DictFix):
    __tablename__ = 'Filesystem'
    host = Column(String(100)) #folio
    system = Column(String(100)) #/data4
    total_space = Column(BigInteger)
    used_space = Column(BigInteger)
    free_space = Column(BigInteger)
    percent_space = Column(Numeric(4,1))
    filesystem_id = Column(String(36), primary_key=True)
    timestamp = Column(BigInteger) #seconds since 1970

class Monitor(Base, ppdata.DictFix):
    __tablename__ = 'Monitor'
    host = Column(String(100))
    base_path = Column(String(100))
    filename = Column(String(100))
    source = Column(String(200))
    status = Column(String(100))
    full_stats = Column(String(200), primary_key=True)
    del_time = Column(BigInteger)
    time_start = Column(BigInteger)
    time_end = Column(BigInteger)
    timestamp = Column(BigInteger)

class Ram(Base, ppdata.DictFix):
    __tablename__ = 'Ram'
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
    ram_id = Column(String(36), primary_key=True)
    timestamp = Column(BigInteger)

class Iostat(Base, ppdata.DictFix):
    __tablename__ = 'Iostat'
    host = Column(String(100))
    device = Column(String(100))
    tps = Column(Numeric(7,2))
    read_s = Column(Numeric(7,2))
    write_s = Column(Numeric(7,2))
    bl_reads = Column(BigInteger)
    bl_writes = Column(BigInteger)
    iostat_id = Column(String(36), primary_key=True)
    timestamp = Column(BigInteger)

class Cpu(Base, ppdata.DictFix):
    __tablename__ = 'Cpu'
    host = Column(String(100))
    cpu = Column(Integer)
    user_perc = Column(Numeric(5,2))
    sys_perc = Column(Numeric(5,2))
    iowait_perc = Column(Numeric(5,2))
    idle_perc = Column(Numeric(5,2))
    intr_s = Column(Integer)
    cpu_id = Column(String(36), primary_key=True)
    timestamp = Column(BigInteger)

class DataBaseInterface(ppdata.DataBaseInterface):
    '''
    Database Interface

    Methods
    -------
    create_db | creates all defined tables
    drop_db | drops all tables from database
    add_entry_dict | adds entry to database using dict as kwarg
    get_entry | gets database object
    '''
    def __init__(self, configfile='~/paperdata/ganglia.cfg'):
        '''
        Unique Interface for the ganglia database

        Parameters
        ----------
        configfile | Optional[str]: ganglia database configuration file --defaults to ~/ganglia.cfg
        '''
        super(DataBaseInterface, self).__init__(configfile=configfile)

    def create_db(self):
        '''
        creates the tables in the database.
        '''
        Base.metadata.bind = self.engine
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
        Base.metadata.create_all()

    def drop_db(self, Base):
        '''
        drops tables in the database

        Parameters
        ----------
        Base | object: Base database object
        '''
        super(DataBaseInterface, self).drop_db(Base)

    def add_entry_dict(self, s, TABLE, entry_dict):
        '''
        create a new entry.

        Parameters
        ----------
        s | object: session object
        TABLE | str: table name
        entry_dict | dict: dict of attributes for object
        '''
        super(DataBaseInterface, self).add_entry_dict(__name__, s, TABLE, entry_dict)

    def get_entry(self, s, TABLE, unique_value):
        '''
        retrieves any object.
        Errors if there are more than one of the same object in the db. This is bad and should
        never happen

        Parameters
        ----------
        s | object: session object
        TABLE | str: table name
        unique_value | int/float/str: primary key value of row

        Returns
        -------
        object: table object
        '''
        super(DataBaseInterface, self).get_entry(__name__, s, TABLE, unique_value)
