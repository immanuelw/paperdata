import h5py
import healpy
import pylab
import numpy as np

if __name__ == '__main__':
	with h5py.File('simulated_map.hdf5','r') as sim_file, h5py.File('new_map.hdf5', 'a') as newfile:
		gmap = sim_file['/map'][:]
		nside = healpy.npix2nside(786432)
		#newfile.create_dataset('/map', data = gmap)
		#newmap = newfile['/map'][:]
		newarr = np.empty((200, 4, 786432), dtype = np.complex128)

		i = 0
		num = 0
		freqnum = 0
		polnum = 0

		theta, phi = np.radians(np.array([90 + 30.72152778, 21.42830556]))
		x, y, z = healpy.ang2vec(theta, phi)
		center = healpy.vec2pix(nside, x, y, z)

		for freq in gmap:
			polnum = 0
			for pol in freq:
				i = 0
				if (polnum == 3):
					continue
				for time in pol:
					#if (np.real(time) == 0): 
					#num += 1
					time_range = 5
					Inten = 100 * (100 / (freqnum + 100)) ** .7
					if (i > center - time_range and i < center):
						if (polnum == 0): 
							newarr[freqnum][polnum][i] = Inten #(i - (center - time_range)) ** 2
						elif (polnum == 1): 
							newarr[freqnum][polnum][i] = .01 * Inten * np.cos(45)
						elif (polnum == 2): 
							newarr[freqnum][polnum][i] = .01 * Inten * np.sin(45)

			#		if (i > center - 1 and i < center + time_range):
			#			if (polnum == 0):
			#				newarr[freqnum][polnum][i] = Inten #(center + time_range - i)**2
			#			elif (polnum == 1):
			#				newarr[freqnum][polnum][i] = .01 * Inten * np.cos(45)
			#			elif (polnum == 2):
			#				newarr[freqnum][polnum][i] = .01 * Inten * np.sin(45)

			#		center = 5100
			#		time_range = 1
			#		if (i > center - time_range and i < center):
			#			newarr[freqnum][polnum][i] = (i - (center - time_range))**2
			#		if (i > center - 1 and i < center + time_range):
			#			newarr[freqnum][polnum][i] = (center + time_range - i)**2
			#		i += 1

			#		center = 5500
			#		time_range = 10
			#		if (i > center - time_range and i < center):
			#			newarr[freqnum][polnum][i] = (i - (center - time_range))**2
			#		if (i > center - 1 and i < center + time_range):
			#			newarr[freqnum][polnum][i] = (center + time_range - i)**2
					i += 1

				polnum += 1
			freqnum += 1
			#healpy.mollview(pol)
			#break
		#pylab.show()

		dset = newfile.create_dataset('map', (200, 4, 786432), dtype=np.complex128)
		dset[...] = newarr[...]
		newmap = newfile['/map'][:]

		#print num
		#num = 0
		freqnum = 0
		for freq in newmap:
			if (freqnum < 5):
				for pol in freq:
		#	for time in pol:
		#		if (np.real(time) == 0):
		#		num += 1
		#	break
		#print num
					healpy.mollview(np.real(pol))
			freqnum += 1
			#break
	pylab.show()
