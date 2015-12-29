'''
paper.site.admin.views

author | Immanuel Washington

Functions
---------
index | shows main page
data_amount | shows data amount table
source_table | shows sources table
filesystem | shows filesystem table
rtp_summary_table | shows rtp summary table
before_request | accesses database
teardown_request | exits database
profile | shows user profile
user_page | shows user page
'''
from flask import render_template, flash, redirect, url_for, request, g, make_response
from flask.ext.login import current_user
import time
from paper.site.flask_app import admin_app as app, admin_db as db
from paper.site.admin import models
from paper.site import db_utils, misc_utils
from paper.data import dbi as pdbi
from paper.ganglia import dbi as pyg

@app.route('/')
@app.route('/index')
@app.route('/index/set/<setName>')
@app.route('/set/<setName>')
def index():
    '''
    start page of the website

    Returns
    -------
    html: index
    '''
    return render_template('index.html')

@app.route('/data_amount', methods = ['GET'])
def data_amount():
    '''
    table of the amount of data

    Returns
    -------
    html: data amount table
    '''
    try:
        data = db_utils.query(database='admin', table='DataAmount', sort_tuples=(('created_on', 'desc'),))[0]
    except:
        data = None

    data_time = hours_sadb = hours_paper = hours_with_data = 'N/A'

    if data is not None:
        data_time = data.created_on
        hours_sadb = data.hours_sadb
        hours_paper = data.hours_paperdata
        hours_with_data = data.hours_with_data

    return render_template('data_amount_table.html', data_time=data_time,
                            hours_sadb=hours_sadb, hours_paper=hours_paper, hours_with_data=hours_with_data)

@app.route('/source_table', methods = ['GET'])
def source_table():
    '''
    table of sources

    Returns
    -------
    html: source table
    '''
    sort_tuples = (('timestamp', 'desc'),)
    output_vars = ('timestamp', 'julian_day')

    try:    
        corr_source = db_utils.query(database='paperdata', table='RTPFile', field_tuples=(('transferred', '==', None),),
                                        sort_tuples=sort_tuples)[0]

        rtp_source = db_utils.query(database='paperdata', table='RTPFile', field_tuples=(('transferred', '==', True),),
                                    sort_tuples=sort_tuples)[0]

        paper_source = db_utils.query(database='paperdata', table='Observation', sort_tuples=sort_tuples)[0]

        source_tuples = (('Correlator', corr_source), ('RTP', rtp_source), ('Folio Scan', paper_source))
    except:
        source_tuples = (('Correlator', None), ('RTP', None), ('Folio Scan', None))

    source_names = tuple(source_name for source_name, _ in source_tuples)
    source_dict = {source_name: {'time': -1, 'day': -1, 'time_segment': 'N/A'} for source_name in source_names}

    for source_name, source in source_tuples:
        if source is not None:
            source_time = int(time.time() - source.timestamp)

            #limiting if seconds or minutes or hours shows up on last report
            source_dict[source_name]['time_segment'] = misc_utils.str_val(source_time)
            source_dict[source_name]['time'] = misc_utils.time_val(source_time)
            source_dict[source_name]['day'] = source.julian_day

    return render_template('source_table.html', source_names=source_names, source_dict=source_dict)

@app.route('/filesystem', methods = ['GET'])
def filesystem():
    '''
    table of filesystems

    Returns
    -------
    html: filesystem table
    '''
    #system_header = (('Free', None), ('File Host', None), ('Last Report', None) , ('Usage Percent', None))
    system_header = (('File Host', None), ('Last Report', None) , ('Usage Percent', None))
    try:
        systems = db_utils.query(database='ganglia', table='Filesystem',
                                    sort_tuples=(('timestamp', 'desc'), ('host', 'asc')), group_tuples=('host',))

        system_names = (system.host for system in systems)
    except:
        systems = (None,)
        system_names = ('pot1', 'pot2', 'pot3', 'folio', 'pot8', 'nas1')

    system_dict = {system_name: {'time': -1, 'used_perc': 100.0, 'time_segment': 'N/A'} for system_name in system_names}
    #system_dict = {system_name: {'time': 'N/A', 'used_perc': 100.0, 'time_segment': 'N/A',
    #                               'stats': 'N/A', 'free_perc': 100.0, 'used_space': 'N/A'}
    #                               for system_name in system_names}
    

    for system in systems:
        if system is not None:
            system_time = int(time.time() - system.timestamp)
            system_name = system.host
            used_perc = system.percent_space
            this_sys = system_dict[system_name]

            #limiting if seconds or minutes or hours shows up on last report
            this_sys['time_segment'] = misc_utils.str_val(system_time)
            this_sys['time'] = misc_utils.time_val(system_time)
            this_sys['used_perc'] = used_perc

            #this_sys['stats'] = ' '.join((system_name, 'stats'))
            #this_sys['free_perc'] = 100 - used_space
            #used_space = system.used_space / 1024.0 ** 3 #convert to GiB
            #this_sys['used_space'] = ' '.join((str(used_space), 'GB'))

    return render_template('filesystem_table.html', system_header=system_header, system_dict=system_dict)

@app.route('/rtp_summary_table', methods = ['POST'])
def rtp_summary_table():
    '''
    summary of rtp status

    Returns
    -------
    html: rtp summary table
    '''
    obs_vars = ('files',)
    file_vars = ('host', 'base_path', 'julian_day', 'transferred', 'new_host', 'new_path', 'timestamp')

    try:
        rtp_obs = db_utils.query(database='paperdata', table='RTPObservation', sort_tuples=(('julian_day', 'desc'),))
        files_list = (obs.files for obs in rtp_obs)
        RTPFiles = (file_obj for file_obj_list in files_list for file_obj in file_obj_list)
        file_list = [{var: getattr(RTPFile, var) for var in file_vars} for RTPFile in RTPFiles]
    except:
        file_list = (None,)

    file_info = {}

    for RTPFile in file_list:
        if not RTPFile is None:
            sa_host_path = ':'.join((RTPFile['host'], RTPFile['path']))
            usa_host_path = ':'.join((RTPFile['new_host'], RTPFile['new_path']))

            julian_day = RTPFile['julian_day']
            transferred = RTPFile['transferred']
            timestamp = RTPFile['time_stamp']

            if julian_day in file_info.keys():
                file_info[julian_day]['count'] += 1
                file_info[julian_day]['transfer']['all'] += 1
            else:
                file_info.update({julian_day: {'count': 1, 'transfer': {'all': 1, 'moved': 0},
                                                'host_path': {'sa_host_path': sa_host_path, 'usa_host_path': usa_host_path},
                                                'activity': 'N/A', 'last_updated': timestamp, 'lst_range': 'N/A'}})
            if transferred:
                file_info[julian_day]['transfer']['moved'] += 1

    try:
        julian_days = file_info.keys()
    except:
        julian_days = (None,)

    output_vars = ('gregorian_day', 'lst_range', 'file_count', 'sa_host_path', 'usa_host_path',
                    'output_host', 'transfer_percent', 'activity', 'last_updated')
    rtp_header = (('Date', 'Observation Date'), ('JD', 'Julian Day'), ('LST Range', 'Local Sidereal Time Range'),
                    ('Count', 'File Count'), ('RTP status', 'Percent of Files transferred'),
                    ('SA Location', 'Host and Path of files on SADB'), ('USA Location', 'Host and Path of files on paperdata'),
                    ('Output Host', 'Host raw files are backup up'), ('Last Activity', 'Last backend action taken'),
                    ('Last Updated', 'Last time action was taken'))
    summary_dict = {julian_day: {var: 0 for var in output_vars} for julian_day in julian_days}
    #need a dict of julian_day: (gregorian_day, lst_range, file_count, sa, usa, output_host?, trans_perc, activity, last_updated)
    for julian_day in julian_days:
        if not julian_day is None:
            file_dict = file_info[julian_day]
            sum_dict = summary_dict[julian_day]
            year, month, day, _ = convert.jd_to_gcal(convert.MJD_0, julian_day - convert.MJD_0)
            sum_dict['gregorian_day'] = '{year}-{month}-{day}'.format(year=year, month=month, day=day)
            sum_dict['lst_range'] = file_dict['lst_range'] 
            sum_dict['file_count'] = file_dict['count']
            sum_dict['sa_host_path'] = file_dict['host_path']['sa_host_path']
            sum_dict['usa_host_path'] = file_dict['host_path']['usa_host_path']
            sum_dict['output_host'] = file_dict['host_path']['output_host']
            sum_dict['transfer_percent'] = file_dict['transfer']['moved'] / file_dict['transfer']['all']
            sum_dict['activity'] = file_dict['activity']
            sum_dict['last_updated'] = file_dict['last_updated']
        
    return render_template('rtp_summary_table.html', rtp_header=rtp_header, summary_dict=summary_dict, output_vars=output_vars)

@app.before_request
def before_request():
    '''
    access database as user before request
    '''
    g.user = current_user
    paper_dbi = pdbi.DataBaseInterface()
    pyg_dbi = pyg.DataBaseInterface()
    try :
        g.paper_session = paper_dbi.Session()
        g.pyg_session = pyg_dbi.Session()
        g.admin_session = db.session
    except Exception as e:
        print('Cannot connect to database - {e}'.format(e))

@app.teardown_request
def teardown_request(exception):
    '''
    exit database after request

    Parameters
    ----------
    exception (exception): exception
    '''
    paper_db = getattr(g, 'paper_session', None)
    pyg_db = getattr(g, 'pyg_session', None)
    admin_db = getattr(g, 'admin_session', None)
    db_list = (paper_db, pyg_db, admin_db)
    for open_db in db_list:
        if open_db is not None:
            open_db.close()

@app.route('/profile')
def profile():
    '''
    access user profile

    Returns
    -------
    html: profile
    OR
    html: redirect for login
    '''
    if (g.user is not None and g.user.is_authenticated()):
        try:
            user = db_utils.query(database='admin', table='User', field_tuples=(('username', '==', g.user.username),),)[0]
            setList = db_utils.query(database='admin', table='Set', field_tuples=(('username', '==', g.user.username),))[0]
        except:
            user = (None,)
            setList = (None,)

        return render_template('profile.html', user=user, sets=setList)
    else:
        return redirect(url_for('login'))

@app.route('/user_page')
def user_page():
    '''
    access user page

    Returns
    -------
    html: user page
    OR
    html: redirect for login
    '''
    if (g.user is not None and g.user.is_authenticated()):
        try:
            user = db_utils.query(database='admin', table='User', field_tuples=(('username', '==', g.user.username),))[0]
            userList = db_utils.query(database='admin', table='User')[0]
            setList = db_utils.query(database='admin', table='Set')[0]
        except:
            user = (None,)
            userList = (None,)
            setList = (None,)

        return render_template('user_page.html', theUser=user, userList=userList, setList=setList)
    else:
        return redirect(url_for('login'))
