#!/flask/bin/python3.4

import psycopg2
import os
import time
import glob
import requests
from datetime import datetime
from paper.site.search import models, db_utils
from paper.site.search.flask_app import db

profiling_mark = None

def write_to_log(msg):
	print(msg)

def profile():
	global profiling_mark
	result = datetime.now() - profiling_mark
	profiling_mark = datetime.now()
	return result.total_seconds()

def log_query_time(var_name):
	write_to_log('{var_name} query ran in {profile_time} seconds'.format(var_name=var_name, profile_time=profile())
	return None

def update():
	utc_now = datetime.utcnow().isoformat()

	baseUTCToGPSURL = 'http://ngas01.ivec.org/metadata/tconv/?utciso='

	gps_now = int(requests.get(baseUTCToGPSURL + utc_now).text)

	profiling_mark = datetime.now()

	#total hours in SADB
	total_sadb_hours = sum(db_utils.query(database='sadb', table='observation',
							field_tuples=(('length', '!=', None),), output_vars=('length',))) / 3600.0

	log_query_time('total_sadb_hours')

	#total hours in paper
	total_paper_hours = sum(db_utils.query(database='paper', table='observation',
								field_tuples=(('length', '!=', None),), output_vars=('length',))) / 3600.0


	log_query_time('total_paper_hours')

	#total that has data in observations
	sadb_obs_rows = db_utils.query(database='sadb', table='observation',
									field_tuples=(('files', '!=', None),), sort_tuples=(('obsnum', 'asc'),),
									output_vars=('length', 'obsnum', 'files'))

	#make tuple of length and file_count for sadb_obs_rows
	sadb_obs_rows = tuple((getattr(sadb_obs, 'length'), getattr(sadb_obs, 'obsnum'), len(getattr(saddb_obs, 'files')))
							for sadb_obs in sadb_obs_rows)

	log_query_time('sadb_obs_rows')

	paper_files_rows = db_utils.query(database='paper', table='file', sort_tuples=(('obsnum', 'asc'),), output_vars=('obsnum',))

	log_query_time('paper_files_rows')

	write_to_log('preparing for the giant iteration where sadb_obs_rows = {sadb_count} and paper_files_rows = {paper_count}'\
					.format(len(sadb_obs_rows), paper_count=len(paper_files_rows)))

	total_data_hours = 0
	no_more_paper_files = False
	match = 0
	no_match = 0
	weird = 0
	not_weird = 0

	#get all the unique observations
	sadb_unique_obs = {sadb_obsnum for _, sadb_obsnum, _ in sadb_obs_rows if sadb_obsnum}
	paper_unique_obs = {paper_obsnum for paper_obsnum in paper_files_rows if paper_obsnum}
	shared_obs = sadb_unique_obs & paper_unique_obs

	#count every file that has a 'shared_obsnum'
	num_paper_files = sum(1 for paper_obsnum in paper_files_rows if paper_obsnum in shared_obs and paper_obsnum is not None)
	match = num_paper_files
	no_match = sum(1 for paper_obsnum in paper_files_rows if paper_obsnum is None)

	for length, sadb_obsnum, sadb_files in sadb_obs_rows:
		num_sadb_files = len(sadb_files)

		if num_paper_files > num_sadb_files:
			print('what the hell! More files in paper! obsid = {obsnum} num_sadb_files = {sadb_num} num_paper_files = {paper_num}'\
							.format(obsnum=sadb_obsnum, sadb_num=num_sadb_files, paper_num=num_paper_files))
			weird += 1
		else:
			not_weird += 1

		total_data_hours += (float(num_paper_files) / float(num_sadb_files)) * length / 3600.0

	print('match = {match} no_match = {no_match}'.format(match=match, no_match=no_match))
	print('weird = {weird} not_weird = {not_weird}'.format(weird=weird, not_weird=not_weird))

	write_to_log('The giant iteration ran in {profile_time} seconds'.format(profile_time=profile()))

	#amount of hours back to check
	time_span = 4
	time_secs = time_span * 3600.0

	#data transfer rate XXXX need to add table or check fields to generate
	#total filesize from rtp_files that have been transferred in the past 4 hours / 4 hours
	data_transfer_total = sum(db_utils.query(database='paper', table='rtp_file',
												field_tuples=(('timestamp', '>=', time.time() - time_secs), ('transferred', '==', True)),
												output_vars=('filesize',)))
	#in MB per second
	data_transfer_rate = data_transfer_total / time_secs

	log_query_time('data_transfer_rate')

	write_to_log('\nTotal Scheduled Hours = {time}'.format(time=round(total_sadb_hours), 6))
	write_to_log('Total Observed Hours = {time}'.format(time=round(total_paper_hours), 6))
	write_to_log('Total Hours that have data = {time}'.format(time=round(total_data_hours), 6))
	write_to_log('Data transfer rate = {time}'.format(time=round(data_transfer_rate), 6))

	graph_datum = getattr(models, 'Data_Amount')(hours_scheduled=total_sadb_hours, hours_observed=total_paper_hours,
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
