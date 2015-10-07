import pylab as plt
import ideal32array as ideal
import numpy as np
import os
import h5py

if __name__ == '__main__':
	tel = ideal.PaperArray()
	tel.zenith = np.radians(np.array([90 + 30.7215277778, 21.4283055556]))

	home = '/data4/paper/mmodes/storage/'
	raw_timestream = os.path.join(home, 'psa32redun/psa904/RREXC_timestream/')
	timestream = os.path.join(home, 'programs/psa32redun_ideal/PAPERdata/pydriver/ts1/timestream_f/')
	hdf5 = '/timestream.hdf5'

	for freq in range(203):
		fnum = str(freq)
		#if (len(fnum) == 2):
		#	fnum = '0' + fnum
		#elif (len(fnum) == 1):
		#	fnum = '00' + fnum

		freq_dir = os.path.join(timestream, fnum)
		if not os.path.exists(freq_dir):
			os.makedirs(freq_dir)

		with h5py.File(os.path.join(freq_dir, hdf5), 'a') as F, h5py.File(os.path.join(raw_timestream, str(freq), hdf5), 'r') as oldF:
			ts = oldF['/timestream']

			F.create_dataset('/baselines', data=tel.baselines)
			F.create_dataset('/feedconj', data=tel.feedconj)
			F.create_dataset('/feedmap', data=tel.feedmap)
			F.create_dataset('/feedmask', data=tel.feedmask)
			F.create_dataset('/uniquepairs', data=tel.uniquepairs)
			F.create_dataset('/phi', data=np.linspace(0, 2 * np.pi, 854, endpoint=False))
			
			F.create_dataset('/timestream', data=ts)

			F.attrs['beamtransfer_path'] = os.path.abspath('pydriver/btdir/')
			F.attrs['ntime'] = 854
