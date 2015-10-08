#!/flask/bin/python3.4

import psycopg2
import os
import time
import glob
import requests
from datetime import datetime
from paper.site.search import models
from paper.site import db_utils
from paper.site.flask_app import search_db as db

profiling_mark = None

def write_to_log(msg):
	'''
	write message to log

	Args:
		msg | str: message
	'''
	print(msg)

def profile():
	'''
	profile the time to run certain functions

	Returns:
		int: time taken by function in seconds
	'''
	global profiling_mark
	result = datetime.now() - profiling_mark
	profiling_mark = datetime.now()

	return result.total_seconds()

def log_query_time(var_name):
	'''
	write time taken to query database to log

	Args:
		var_name | str: variable name
	'''
	write_to_log('{var_name} query ran in {profile_time} seconds'.format(var_name=var_name, profile_time=profile())

	return None

def update():
	'''
	pull data and update database
	'''
	utc_now = datetime.utcnow().isoformat()

	baseUTCToGPSURL = 'http://ngas01.ivec.org/metadata/tconv/?utciso='

	gps_now = int(requests.get(baseUTCToGPSURL + utc_now).text)

	profiling_mark = datetime.now()

	#total hours in SADB
	total_sadb_hours = sum(getattr(OBS, 'length') for OBS in db_utils.query(database='sadb', table='observation',
                            field_tuples=(('length', '!=', None),))) / 3600.0

	log_query_time('total_sadb_hours')

	#total hours in paper
	total_paperdata_hours = sum(getattr(OBS, 'length') for OBS in db_utils.query(database='paperdata', table='observation',
                            field_tuples=(('length', '!=', None),))) / 3600.0

	log_query_time('total_paperdata_hours')

	#total that has data in observations
	sadb_obs_rows = db_utils.query(database='sadb', table='observation',
									field_tuples=(('files', '!=', None),), sort_tuples=(('obsnum', 'asc'),))

	#make tuple of length and file_count for sadb_obs_rows
	sadb_obs_rows = tuple((getattr(sadb_obs, 'length'), getattr(sadb_obs, 'obsnum'), len(getattr(saddb_obs, 'files')))
							for sadb_obs in sadb_obs_rows)

	log_query_time('sadb_obs_rows')

	paperdata_files_rows = db_utils.query(database='paperdata', table='file', sort_tuples=(('obsnum', 'asc'),))

	log_query_time('paperdata_files_rows')

	write_to_log('preparing for the giant iteration where sadb_obs_rows = {sadb_count} and paperdata_files_rows = {paperdata_count}'\
					.format(len(sadb_obs_rows), paperdata_count=len(paperdata_files_rows)))

	total_data_hours = 0
	no_more_paperdata_files = False
	match = 0
	no_match = 0
	weird = 0
	not_weird = 0

	#get all the unique observations
	sadb_unique_obs = {sadb_obsnum for _, sadb_obsnum, _ in sadb_obs_rows if sadb_obsnum}
	paperdata_unique_obs = {paperdata_obsnum for paperdata_obsnum in paperdata_files_rows if paperdata_obsnum}
	shared_obs = sadb_unique_obs & paperdata_unique_obs

	#count every file that has a 'shared_obsnum'
	num_paperdata_files = sum(1 for paperdata_obsnum in paperdata_files_rows if paperdata_obsnum in shared_obs and paperdata_obsnum is not None)
	match = num_paperdata_files
	no_match = sum(1 for paperdata_obsnum in paperdata_files_rows if paperdata_obsnum is None)

	for length, sadb_obsnum, sadb_files in sadb_obs_rows:
		num_sadb_files = len(sadb_files)

		if num_paperdata_files > num_sadb_files:
			print('what the hell! More files in paper! obsid = {obsnum} num_sadb_files = {sadb_num} num_paperdata_files = {paperdata_num}'\
							.format(obsnum=sadb_obsnum, sadb_num=num_sadb_files, paperdata_num=num_paperdata_files))
			weird += 1
		else:
			not_weird += 1

		total_data_hours += (float(num_paperdata_files) / float(num_sadb_files)) * length / 3600.0

	print('match = {match} no_match = {no_match}'.format(match=match, no_match=no_match))
	print('weird = {weird} not_weird = {not_weird}'.format(weird=weird, not_weird=not_weird))

	write_to_log('The giant iteration ran in {profile_time} seconds'.format(profile_time=profile()))

	#amount of hours back to check
	time_span = 4
	time_secs = time_span * 3600.0

	#data transfer rate XXXX need to add table or check fields to generate
	#total filesize from rtp_files that have been transferred in the past 4 hours / 4 hours
	data_transfer_total = sum(db_utils.query(database='paperdata', table='rtp_file',
												field_tuples=(('timestamp', '>=', time.time() - time_secs), ('transferred', '==', True)),
												output_vars=('filesize',)))
	data_transfer_total = sum(getattr(FILE, 'filesize') for FILE in db_utils.query(database='paperdata', table='rtp_file',
							field_tuples=(('timestamp', '>=', time.time() - time_secs), ('transferred', '==', True))))
	#in MB per second
	data_transfer_rate = data_transfer_total / time_secs

	log_query_time('data_transfer_rate')

	write_to_log('\nTotal Scheduled Hours = {time}'.format(time=round(total_sadb_hours), 6))
	write_to_log('Total Observed Hours = {time}'.format(time=round(total_paperdata_hours), 6))
	write_to_log('Total Hours that have data = {time}'.format(time=round(total_data_hours), 6))
	write_to_log('Data transfer rate = {time}'.format(time=round(data_transfer_rate), 6))

	graph_datum = getattr(models, 'Data_Amount')(hours_scheduled=total_sadb_hours, hours_observed=total_paperdata_hours,
												hours_with_data=total_data_hours, data_transfer_rate=data_transfer_rate)

	db.session.add(graph_datum)
	db.session.commit()

if __name__ == '__main__':

	write_to_log('\n-- {timestamp} -- \n' .format(timestamp=datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f%z')))

	# Establish the database connection
	profiling_mark = datetime.now()

	try:
		update()
	except:
		except Exception as e:	
			write_to_log('Cannot connect to some database - {e}'.format(e=e))
			exit(1)
