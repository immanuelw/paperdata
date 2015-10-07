'''----------------------------------------------------------------------------
	basetofreq.py

		Written by: Jason Ling
		Last Modified: December 3rd, 2014

		execution: 
		python basetofreq.py
		output: 
		converted hdf5 files from intermediate baseline hdf5 files
	notes:
		runs on the order of ~12 hours
		highly recommended to use a client that can continue to run
			after logging out (like nohup)

----------------------------------------------------------------------------'''
# Import statements
import numpy as N
import aipy as A
import h5py
import sys
import os
from glob import glob
import csv

#print 'Initializing program'
# Declares and initiates variables
directory = 'psa904/'
raw_data = os.path.join('/data4/raw_data/Sep2011/', directory)
home = '/data4/paper/mmodes/'
timestream = os.path.join(home, 'storage/psa32redun/', directory, 'RREXC_timestream/')
basepath = os.path.join(home, 'storage/psa32redun/', directory, 'RREXC_baseline/')
#freqpath = os.path.join(home, 'storage/psa32dualpol/', directory, 'RREC_timestreams/') #maybe later
#timestream = '/data2/home/lingj/anaconda/pkgs/driftscan-master/examples/paper/pydriver/ts1/timestream_f/'
baseline1 = 0
baseline2 = 0
base = N.empty((528, 2), dtype=int)
polarization = N.array(['yy','xx','xy','yx'])
suffix = '.uvcRREXC'
i = 0
first = False
time = 0
polnum = 0
spec_num = 0
freq = 0


'''----------------------------------------------
	SECTION: Writes Timestream
--------------------------------------------'''

#SK SK SK

if __name__ == '__main__': 
	#base1, base2, dist = N.loadtxt('sorted_baselines.csv', delimiter=',', unpack=True)
	with open('UniqueBaselinesSorted.csv','r') as csvfile:
		reader = csv.reader(csvfile, delimiter=',')
		basefiles = [''.join((basepath, row[0], '.hdf5')) for row in reader]

	#might shout at us bcse numpy arrays
	#basefiles = [''.join((basepath, str(int(val_1)), '_', str(int(val_2)), '.hdf5')) for val_1, val_2 in zip(base1, base2)]

	#SK SK SK

	# Obtains the dimensions of the timestream that will be used
	with h5py.File(basefiles[0], 'r') as example:
		dimen = example['/data'][:].shape # (203, 4, 1988)
	stream = N.zeros((4 * len(basefiles), dimen[2]), dtype=N.complex128) #4*numBLs, time

	# Loop that iterates through all frequencies and sets up timestream
	for freq in range(dimen[0]):
		print freq
		sys.stdout.flush()
		freqpath = ''.join((timestream, str(freq), '/'))

		# Iterates through all of the baseline hdf5 files made in createstream.py
		for k, fl in enumerate(basefiles):
			#print k, fl
			# Opens file to read
			with h5py.File(fl, 'r') as stream_file:
				indata = stream_file['/data'][:]

				# Isolates the correct frequency in the opened read file
				indata_our_freq = indata[freq, :, :]
				
				for pol in range(3):
					timestream_our_pol = indata_our_freq[pol, :]
					stream[4 * k + pol][:] = timestream_our_pol
			
		# Makes the frequency directories
		if not os.path.exists(freqpath):
			os.makedirs(freqpath)

		# Prepares the output file
		with h5py.File(''.join((freqpath, 'timestream.hdf5')), 'a') as out:
			dset = out.create_dataset('timestream', (4 * len(basefiles), dimen[2]), dtype=N.complex128)
			dset[...] = stream[...]
