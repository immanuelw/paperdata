from datetime import datetime
from paper.convert import gcal2jd

def get_set_strings():
	'''
	output a list of set strings for main filterable fields

	Returns
	-------
	list: set strings for polarization, era_type, host, and filetype
	'''
	pol_strs = ('all', 'xx', 'xy', 'yx', 'yy')
	era_type_strs = ('all',)
	host_strs = ('all', 'pot1', 'pot2', 'pot3', 'folio', 'pot8', 'nas1')
	filetype_strs = ('all', 'uv', 'uvcRRE', 'npz')

	return pol_strs, era_type_strs, host_strs, filetype_strs

def get_jd_from_datetime(start_time=None, end_time=None):
	'''
	generates julian date from datetime -- either object or string

	Parameters
	----------
	start_time (object/str): start time --defaults to None
	end_time (object/str): end time --defaults to None

	Returns
	-------
	float: time start of julian date
	float: time end of julian date
	'''
	time_start = None
	time_end = None
	if isinstance(start_time, str):
		start_time = datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%S')
	if isinstance(end_time, str):
		end_time = datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%S')
	if start_time is not None:
		time_start = gcal2jd(start_time.year, start_time.month, start_time.day, start_time.hour, start_time.minute, start_time.second)
	if end_time is not None:
		time_end = gcal2jd(end_time.year, end_time.month, end_time.day, end_time.hour, end_time.minute, end_time.second)
	if time_end is None:
		return time_start

	return time_start, time_end

def get_utc_from_datetime(date_time):
	'''
	generates utc in seconds from datetime object

	Parameters
	----------
	date_time | object: datetime object

	Returns
	-------
	int: utc in seconds
	'''
	utc = (date_time - datetime(1970, 1, 1)).total_seconds()

	return utc
