from sqlalchemy import Table, Column, String, Integer, ForeignKey, Float, func, Boolean, DateTime, Enum, BigInteger, Numeric, Text
from sqlalchemy import event, DDL
from sqlalchemy.orm import relationship, backref
import datetime
import logging
import paper as ppdata

Base = ppdata.Base
logger = logging.getLogger('monitor')

neighbors = Table("neighbors", Base.metadata,
                  Column("low_neighbor_id", String(100), ForeignKey("observation.obsnum"), primary_key=True),
                  Column("high_neighbor_id", String(100), ForeignKey("observation.obsnum"), primary_key=True)
                  )


class Observation(Base):
    __tablename__ = 'observation'
    # date = Column(BigInteger)  # Jon: Changed this to a biginteger for now... Though I can probably just pad my date
    date = Column(String(100))  # Jon: Changed this to a biginteger for now... Though I can probably just pad my date
    date_type = Column(String(100))
    pol = Column(String(4))
    # JON: removed default=updateobsnum, late, should figure out how to just override the alchamy base class thinggie.
    # obsnum = Column(BigInteger, default=updateobsnum, primary_key=True)
    # obsnum = Column(BigInteger, primary_key=True)
    obsnum = Column(String(100), primary_key=True)
    # status = Column(Enum(*FILE_PROCESSING_STAGES, name='FILE_PROCESSING_STAGES'))
    # Jon: There may be a very good reason to not just make this a string and I'm sure I will find out what it is soon enough
    status = Column(String(200))
    # last_update = Column(DateTime,server_default=func.now(),onupdate=func.current_timestamp())
    length = Column(Float)  # length of observation in fraction of a day
    currentpid = Column(Integer)
    stillhost = Column(String(100))
    stillpath = Column(String(200))
    outputpath = Column(String(200))
    outputhost = Column(String(100))
    current_stage_in_progress = Column(String(200))
    current_stage_start_time = Column(DateTime)
    high_neighbors = relationship("Observation",
                                  secondary=neighbors,
                                  primaryjoin=obsnum == neighbors.c.low_neighbor_id,
                                  secondaryjoin=obsnum == neighbors.c.high_neighbor_id,
                                  backref="low_neighbors",
                                  cascade="all, delete-orphan",
                                  single_parent=True)


class File(Base):
    __tablename__ = 'file'
    filenum = Column(Integer, primary_key=True)
    filename = Column(String(200))
    #path_prefix = Column(String(200))
    host = Column(String(100))
    obsnum = Column(String(100), ForeignKey('observation.obsnum'))
    # this next line creates an attribute Observation.files which is the list of all
    #  files associated with this observation
    observation = relationship(Observation, backref=backref('files', uselist=True), cascade="all, delete-orphan", single_parent=True)
    md5sum = Column(Integer)


class Log(Base):
    __tablename__ = 'log'
    lognum = Column(BigInteger, primary_key=True)
#    obsnum = Column(BigInteger, ForeignKey('observation.obsnum'))
    # Jon: obsnum = Column(String(100), ForeignKey('observation.obsnum'))
    # Jon: There may be a very good reason to not just make this a string and I'm sure I will find out what it is soon enough
    obsnum = Column(String(100))
    stage = Column(String(200))
    # stage = Column(Enum(*FILE_PROCESSING_STAGES, name='FILE_PROCESSING_STAGES'))
    exit_status = Column(Integer)
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    timestamp = Column(DateTime, nullable=False, default=func.current_timestamp())
    logtext = Column(Text)
    # observation = relationship(Observation, backref=backref('logs', uselist=True), cascade="all, delete-orphan", single_parent=True)


class Still(Base):
    __tablename__ = 'still'
    hostname = Column(String(100), primary_key=True)
    ip_addr = Column(String(50))
    port = Column(BigInteger)
    data_dir = Column(String(200))
    last_checkin = Column(DateTime, default=datetime.datetime.now(), onupdate=datetime.datetime.now())
    status = Column(String(100))
    current_load = Column(Integer)
    number_of_cores = Column(Integer)  # Jon : Placeholder for future expansion
    free_memory = Column(Integer)      # Jon : Placeholder for future expansion
    total_memory = Column(Integer)     # Jon : Placeholder for future expansion
    cur_num_of_tasks = Column(Integer)
    max_num_of_tasks = Column(Integer)


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
    def __init__(self, configfile='~/paperdata/rtp.cfg'):
        '''
        Unique Interface for the paperdata database

        Parameters
        ----------
        configfile | str: paperdata database configuration file
        '''
        super(DataBaseInterface, self).__init__(configfile=configfile)

