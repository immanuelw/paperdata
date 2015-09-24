import glob
import json
import decimal
import numpy as np
'''
Generates gain and delay from coeff*.npz files and phase_*.npz files
'''

def decimal_default(obj):
	'''
	fixes decimal issue with json module
	'''
	if isinstance(obj, decimal.Decimal):
		return float(obj)

def json_data(dbo, full_dict):
	'''
	dumps list of objects into a json file

	input: filename, list of database objects
	'''
	with open(dbo, 'w') as f:
		data = [full_dict]
		json.dump(data, f, sort_keys=True, indent=1, default=decimal_default)
	return None

def gen_gain(file_path):
	gain_data = np.load(file_path)['idk']
	gain = {ant_num: {'x': 0, 'y': 0} for ant_num in range(32)}
	for pol_num, polarization in enumerate(('x', 'y')):
		for ant_num in range(32):
			gain[ant_num][polarization] = np.polyfit(?, gain_data[pol_num, :, ant_num]), 1)
	return gain

def gen_delay(file_path):
	delay_data = np.load(file_path)['phase']
	delay = {ant_num: {'x': 0, 'y': 0} for ant_num in range(32)}
	for pol_num, polarization in enumerate(('x', 'y')):
		for ant_num in range(32):
			delay[ant_num][polarization] = np.polyfit(?, delay_data[pol_num, :, ant_num]), 1)
	return delay

if __name__ == '__main__':
	output_base = '/data4/paper/paper_gd/gd_{}.json'
	gain_base = '/data4/paper/exchange_sa_upenn/abscal_paramters/amp_par/coeff_{}.npz'
	delay_base = '/data4/paper/exchange_sa_upenn/abscal_paramters/phase_soln/npzFiles/phase_{}.npz'
	abscal_files = glob.glob('/data4/paper/exchange_sa_upenn/abscal_data/zen*uvcRRECXRM.uv')
	for file_path in abscal_files:
		julian_date = file_path.split('/')[-1].split('zen.')[1].split('.uvcRRECXRM.uv')[0]

		gain = gen_gain(gain_base.format(julian_date))
		delay = gen_delay(delay_base.format(julian_date))

		#combine into one giant dict
		full_dict = {'amps': gain, 'delays': delay}

		output_path	= output_base.format(julian_date)
		json_data(output_path, full_dict)
