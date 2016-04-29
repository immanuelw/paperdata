'''
paper.site.search.views

author | Immanuel Washington

Functions
---------
time_fix | fixes times for observation input
index | shows main page
obs_table | shows observation table
save_obs | generates observation json file
file_table | shows file table
save_files | generates file json file
before_request | accesses database
teardown_request | exits database
data_summary_table | shows data summary table
day_summary_table | shows day summary table
'''
from flask import render_template, flash, redirect, url_for, request, g, make_response, Response
from flask.ext.login import current_user
import json
import time
from datetime import datetime
import paper as ppdata
from paper.site.flask_app import search_app as app, search_db as db
from paper.site.search import models
from paper.site import db_utils, misc_utils
from paper.data import dbi as pdbi
from paper.ganglia import dbi as pyg
from sqlalchemy import func
import plotly.plotly as py
import plotly.graph_objs as go
import numpy as np

def time_fix(jdstart, jdend, starttime=None, endtime=None):
    '''
    fixes times for observations

    Parameters
    ----------
    jdstart | str: starting time of julian date
    jdend | str: ending time of julian date
    starttime | str: string of start time --defaults to None
    endtime | str: string of end time --defaults to None

    Returns
    -------
    tuple:
        float(5): julian date start time
        float(5): julian date end time
    '''
    try:
        jd_start = round(float(jdstart), 5)
        jd_end = round(float(jdend), 5)
    except:
        jd_start = None
        jd_end = None

    if jd_start == None:
        startdatetime = datetime.strptime(starttime, '%Y-%m-%dT%H:%M:%SZ')
        enddatetime = datetime.strptime(endtime, '%Y-%m-%dT%H:%M:%SZ')
        start_utc, end_utc = misc_utils.get_jd_from_datetime(startdatetime, enddatetime)
    else:
        start_utc, end_utc = jd_start, jd_end

    return start_utc, end_utc

@app.route('/', methods=['GET', 'POST'])
@app.route('/index', methods=['GET', 'POST'])
def index():
    '''
    start page of the website
    grabs time data

    Returns
    -------
    html: index
    '''
    polarization_dropdown, era_type_dropdown, host_dropdown, filetype_dropdown = misc_utils.get_dropdowns()

    jdstart = request.args.get('jd_start', 2455903)
    jdend = request.args.get('jd_end', 2455904)
    starttime = request.args.get('starttime', None)
    endtime = request.args.get('endtime', None)

    polarization = request.args.get('polarization', 'all')
    era_type = request.args.get('era_type', 'None')
    host = request.args.get('host', 'folio')
    filetype = request.args.get('filetype', 'uv')

    start_utc, end_utc = time_fix(jdstart, jdend, starttime, endtime)

    return render_template('index.html',
                            polarization_dropdown=polarization_dropdown, era_type_dropdown=era_type_dropdown,
                            host_dropdown=host_dropdown, filetype_dropdown=filetype_dropdown,
                            start_utc=start_utc, end_utc=end_utc,
                            polarization=polarization, era_type=era_type,
                            host=host, filetype=filetype)

@app.route('/data_hist', methods = ['POST'])
def data_hist():
    y = np.random.randn(500)

    data = [go.Histogram(y=y)]
    plot_url = py.plot(data, filename='horizontal-histogram')

@app.route('/obs_table', methods = ['POST'])
def obs_table():
    '''
    generate observation table for histogram bar

    Returns
    -------
    html: observation table
    '''
    jdstart = request.args.get('jd_start', 2455903)
    jdend = request.args.get('jd_end', 2455904)
    starttime = request.args.get('starttime', None)
    endtime = request.args.get('endtime', None)

    start_utc, end_utc = time_fix(jdstart, jdend, starttime, endtime)

    polarization = request.args.get('polarization', 'all')
    era_type = request.args.get('era_type', 'None')

    fixed_et = None if era_type == 'None' else era_type
    output_vars = ('obsnum', 'julian_date', 'polarization', 'length')

    try:
        response = db_utils.query(database='paperdata', table='Observation', 
                                    field_tuples=(('time_start', '>=', start_utc), ('time_end', '<=', end_utc),
                                    ('polarization', None if polarization == 'any' else '==', polarization),
                                    ('era_type', None if era_type in ('all', 'None') else '==', era_type)),
                                    sort_tuples=(('time_start', 'asc'),))
        log_list = [{var: getattr(obs, var) for var in output_vars} for obs in response]
    except:
        log_list = []

    return render_template('obs_table.html',
                           log_list=log_list, output_vars=output_vars,
                           start_time=start_utc, end_time=end_utc,
                           polarization=polarization, era_type=era_type)

@app.route('/save_obs', methods = ['GET'])
def save_obs():
    '''
    saves observations as json

    Returns
    -------
    html: json file
    '''
    start_utc = float(request.args['start_utc'])
    end_utc = float(request.args['end_utc'])

    polarization = request.args['polarization']
    era_type = request.args['era_type']

    fixed_et = None if era_type == 'None' else era_type

    try:
        response = db_utils.query(database='paperdata', table='Observation', 
                                    field_tuples=(('time_start', '>=', start_utc), ('time_end', '<=', end_utc),
                                    ('polarization', None if polarization == 'any' else '==', polarization),
                                    ('era_type', None if era_type in ('all', 'None') else '==', era_type)),
                                    sort_tuples=(('time_start', 'asc'),))

        entry_list = [obs.to_dict() for obs in response]
    except:
        entry_list = []

    return Response(response=json.dumps(entry_list, sort_keys=True, indent=4, default=ppdata.decimal_default),
                    status=200, mimetype='application/json', headers={'Content-Disposition': 'attachment; filename=obs.json'})

@app.route('/file_table', methods = ['GET', 'POST'])
def file_table():
    '''
    generate file table for histogram bar

    Returns
    -------
    html: file table
    '''
    jdstart = request.args.get('jd_start', 2455903)
    jdend = request.args.get('jd_end', 2455904)
    starttime = request.args.get('starttime', None)
    endtime = request.args.get('endtime', None)

    host = request.args.get('host', 'folio')
    filetype = request.args.get('filetype', 'uv')

    start_utc, end_utc = time_fix(jdstart, jdend, starttime, endtime)

    output_vars = ('host', 'source', 'obsnum', 'filesize')

    try:
        dbi = pdbi.DataBaseInterface()
        obs_table = pdbi.Observation
        file_table = pdbi.File
        with dbi.session_scope() as s:
            all_obs_list = s.query(file_table).join(obs_table).filter(obs_table.time_start >= start_utc).filter(obs_table.time_end <= end_utc)

            if host != 'all':
                all_obs_list = all_obs_list.filter(file_table.host == host)
            if filetype != 'all':
                all_obs_list = all_obs_list.filter(file_table.filetype == filetype)

            log_list = [{var: getattr(paper_file, var) for var in output_vars}
                        for paper_file in all_obs_list.order_by(obs_table.time_start.asc()).all()]
    except:
        log_list = []

    return render_template('file_table.html',
                           log_list=log_list, output_vars=output_vars,
                           start_time=start_utc, end_time=end_utc,
                           host=host, filetype=filetype)

@app.route('/save_files', methods = ['GET'])
def save_files():
    '''
    saves file metadata as json

    Returns
    -------
    html: json file
    '''
    start_utc = float(request.args['start_utc'])
    end_utc = float(request.args['end_utc'])

    host = request.args['host']
    filetype = request.args['filetype']

    try:
        dbi = pdbi.DataBaseInterface()
        obs_table = pdbi.Observation
        file_table = pdbi.File
        with dbi.session_scope() as s:
            all_obs_list = s.query(file_table).join(obs_table).filter(obs_table.time_start >= start_utc).filter(obs_table.time_end <= end_utc)

            if host != 'all':
                all_obs_list = all_obs_list.filter(file_table.host == host)
            if filetype != 'all':
                all_obs_list = all_obs_list.filter(file_table.filetype == filetype)

            entry_list = [paper_file.to_dict() for paper_file in all_obs_list.order_by(obs_table.time_start.asc()).all()]
    except:
        entry_list = []

    return Response(response=json.dumps(entry_list, sort_keys=True, indent=4, default=ppdata.decimal_default),
                    status=200, mimetype='application/json', headers={'Content-Disposition': 'attachment; filename=file.json'})

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
        g.search_session = db.session
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
    search_db = getattr(g, 'search_session', None)
    db_list = (paper_db, pyg_db, search_db)
    for open_db in db_list:
        if open_db is not None:
            open_db.close()

@app.route('/data_summary_table', methods=['POST'])
def data_summary_table():
    '''
    summary of data in main databases

    Returns
    -------
    html: data summary table
    '''
    jdstart = request.args.get('jd_start', 2455903)
    jdend = request.args.get('jd_end', 2455904)
    starttime = request.args.get('starttime', None)
    endtime = request.args.get('endtime', None)

    start_utc, end_utc = time_fix(jdstart, jdend, starttime, endtime)

    pol_strs, era_type_strs, host_strs, filetype_strs = misc_utils.get_set_strings()
    obs_map = {pol_str: {era_type_str: {'obs_count': 0, 'obs_hours': 0} for era_type_str in era_type_strs} for pol_str in pol_strs}
    file_map = {host_str: {filetype_str: {'file_count': 0} for filetype_str in filetype_strs} for host_str in host_strs}

    dbi = pdbi.DataBaseInterface()
    with dbi.session_scope() as s:
        obs_table = pdbi.Observation
        response = s.query(obs_table.polarization, obs_table.era_type, func.sum(obs_table.length), func.count(obs_table))\
                            .filter(obs_table.time_start >= start_utc).filter(obs_table.time_end <= end_utc)\
                            .group_by(obs_table.polarization, obs_table.era_type).all()
        for polarization, era_type, length, count in response:
            obs_map[polarization][str(era_type)].update({'obs_count': count , 'obs_hours': length})

        file_table = pdbi.File
        response = s.query(file_table.host, file_table.filetype, func.count(file_table)).group_by(file_table.host, file_table.filetype).all()
        for host, filetype, count in response:
            file_map[host][filetype].update({'file_count': count})

    all_obs_strs = pol_strs + era_type_strs
    obs_total = {all_obs_str: {'count': 0, 'hours': 0} for all_obs_str in all_obs_strs}

    for pol in pol_strs:
        for era_type in era_type_strs:
            obs_count = obs_map[pol][era_type]['obs_count']
            obs_hours = obs_map[pol][era_type]['obs_hours']
            obs_total[era_type]['count'] += obs_count
            obs_total[era_type]['hours'] += obs_hours
            obs_total[pol]['count'] += obs_count
            obs_total[pol]['hours'] += obs_hours

    all_file_strs = host_strs + filetype_strs
    file_total = {all_file_str: {'count': 0} for all_file_str in all_file_strs}

    for host in host_strs:
        for filetype in filetype_strs:
            file_count = file_map[host][filetype]['file_count']
            file_total[filetype]['count'] += file_count
            file_total[host]['count'] += file_count

    #gets rid of useless columns
    no_obs = {era_type: {'obs_count': 0, 'obs_hours': 0} for era_type in era_type_strs}
    pol_strs = tuple(pol for pol, era_type_dict in obs_map.items() if era_type_dict != no_obs)

    no_files = {filetype: {'file_count': 0} for filetype in filetype_strs}
    host_strs = tuple(host for host, filetype_dict in file_map.items() if filetype_dict != no_files)

    return render_template('data_summary_table.html',
                            pol_strs=pol_strs, era_type_strs=era_type_strs, host_strs=host_strs, filetype_strs=filetype_strs,
                            obs_map=obs_map, obs_total=obs_total, file_map=file_map, file_total=file_total)

@app.route('/day_summary_table', methods=['POST'])
def day_summary_table():
    '''
    summary of data in main databases

    Returns
    -------
    html: day summary table
    '''
    jdstart = request.args.get('jd_start', 2455903)
    jdend = request.args.get('jd_end', 2455904)
    starttime = request.args.get('starttime', None)
    endtime = request.args.get('endtime', None)

    start_utc, end_utc = time_fix(jdstart, jdend, starttime, endtime)

    dbi = pdbi.DataBaseInterface()
    with dbi.session_scope() as s:
        obs_table = pdbi.Observation
        response = s.query(obs_table.julian_day, func.count(obs_table))\
                            .filter(obs_table.time_start >= start_utc).filter(obs_table.time_end <= end_utc)\
                            .group_by(obs_table.julian_day).order_by(obs_table.julian_day.asc()).all()
        day_map = tuple((julian_day, count) for julian_day, count in response)

    return render_template('day_summary_table.html', day_map=day_map)
