import numpy as np
from scipy.special import jn
import healpy
from healpy import *
from cora.util import coord
from drift.core import telescope

def beam_circular(angpos, zenith, diameter):
	'''Beam pattern for a uniformly illuminated circular dish.
	
	Parameters
	----------
	angpos : np.ndarray
		Array of angular positions
	zenith : np.ndarray
		Co-ordinates of the zenith.
	diameter : scalar
		Diameter of the dish (in units of wavelength).
	
	Returns
	-------
	beam : np.ndarray
		Beam pattern at each position in angpos.
	'''
	
	def jinc(x):
		return 0.5 * (jn(0, x) + jn(2, x))

	x = (1.0 - coord.sph_dot(angpos, zenith) ** 2) ** 0.5 * np.pi * diameter
	
	return 2 * jinc(x)

class PaperArray(telescope.SimplePolarisedTelescope):
	# Set band properties (overriding baseclass)
	freq_lower = 100.0
	freq_upper = 200.0
	num_freq = 5

	# Properties for the Dish Array -- give it enough space for a 64-dish array
	dish_width = 3.1 
	#PAPER FWHM = 45 degrees at 150 MHz (2 meters), so using FWHM_Airy = 1.22 lambda/D (double check I get D = 3.1 m. 
	gridu = 4
	gridv = 8

	# Give the widths in the U and V directions in metres (used for
	# calculating the maximum l and m)
	@property
	def u_width(self):
		return self.dish_width

	@property
	def v_width(self):
		return self.dish_width


	# Implement the X and Y beam patterns (assuming all feeds are identical).
	# These need to return a vector for each position on the sky
	# (self._angpos) in thetahat, phihat coordinates.
	def beamx(self, feed, freq):
		# Calculate beam amplitude
		beam = beam_circular(self._angpos, self.zenith,
							 self.dish_width / self.wavelengths[freq])

		# Add a vector direction to beam - X beam is EW (phihat)
		beam = beam[:, np.newaxis] * np.array([0.0, 1.0])

		return beam

	def beamy(self, feed, freq):
		# Calculate beam amplitude
		beam = beam_circular(self._angpos, self.zenith,
							 self.dish_width / self.wavelengths[freq])

		# Add a vector direction to beam - Y beam is NS (thetahat)
		# Fine provided beam does not cross a pole.
		beam = beam[:, np.newaxis] * np.array([1.0, 0.0])

		return beam


	# Set the feed array of feed positions (in metres EW, NS)
	@property
	def _single_feedpositions(self):
		N, x, y = np.loadtxt('psa32redun_ideal.dat', unpack=True)
		
		pos = np.zeros((self.gridu, self.gridv, 2))

		for i in range(self.gridu):
			for j in range(self.gridv):
				pos[i, j, 0] = i * self.dish_width
				pos[i, j, 1] = j * self.dish_width
		
		
		A =  pos.reshape((self.gridu * self.gridv, 2))
		
		for i in range(32):
			A[i, 0] = x[i]
			A[i, 1] = y[i]
		
		return A
