import sys, os
import cmath
from casa import table as tb
from casa import image as ia
from casa import flagdata, clean
from casa import *
import numpy as np
import shutil
#import pyfits as pf

def bandpass_normalize(bandpass_table, bandpass_table_inv):
	shutil.copytree(bandpass_table, bandpass_table_inv)
	tb.open(bandpass_table_inv, nomodify=False)
	gain = tb.getcol('CPARAM')
	gain_norm = gain

	#for each antenna and for each channel of the bandpass I divide out by the modulo of the complex number
	for i in range(0, gain.shape[2]):
		for j in range(0, gain.shape[1]):
			a = gain[0, j, i]

			#if the real part of the antenna gain is set to 1 it means that that antenna a/o channel is flag, so don't bother looking at it
			if a.real != 1:
				gain_norm[0, j, i] = gain_norm[0, j, i] / abs(gain_norm[0, j, i]) 
				gain_norm[1, j, i] = gain_norm[1, j, i] / abs(gain_norm[1, j, i])
	#			print abs(gain_norm[i,j,0]),abs(gain[i,j,0]),i,j

	# put back the normalized bandpass
	tb.putcol('CPARAM', gain_norm)
	tb.close()
	tb.done()

def clean2fits(infile_dict, outfile_dict):
	for in_name in outfile_dict.keys()
		with infile_dict[in_name] as ia:
			ia.tofits(outfile_dict[in_name], overwrite=True)

if __name__ == '__main__':
	# look for there files on elwood
	filenames = ['zen.2455819.50285.uvcRRECXRM.MS']
	#'zen.2455819.50981.uvcRRECXRM.MS', 'zen.2455819.51677.uvcRRECXRM.MS', 'zen.2455819.52373.uvcRRECXRM.MS', 'zen.2455819.53069.uvcRRECXRM.MS', 
	#'zen.2455819.53765.uvcRRECXRM.MS', 'zen.2455819.54461.uvcRRECXRM.MS', 'zen.2455819.55157.uvcRRECXRM.MS', 'zen.2455819.55853.uvcRRECXRM.MS', 
	#'zen.2455819.56548.uvcRRECXRM.MS', 'zen.2455819.57244.uvcRRECXRM.MS', 'zen.2455819.57940.uvcRRECXRM.MS', 'zen.2455819.58636.uvcRRECXRM.MS', 
	#'zen.2455819.59332.uvcRRECXRM.MS', 'zen.2455819.60028.uvcRRECXRM.MS', 'zen.2455819.60724.uvcRRECXRM.MS', 'zen.2455819.61420.uvcRRECXRM.MS', 
	#'zen.2455819.62116.uvcRRECXRM.MS', 'zen.2455819.62812.uvcRRECXRM.MS', 'zen.2455819.63508.uvcRRECXRM.MS', 'zen.2455819.64204.uvcRRECXRM.MS', 
	#'zen.2455819.64900.uvcRRECXRM.MS', 'zen.2455819.65596.uvcRRECXRM.MS', 'zen.2455819.66292.uvcRRECXRM.MS', 'zen.2455819.66988.uvcRRECXRM.MS', 
	#'zen.2455819.67684.uvcRRECXRM.MS', 'zen.2455819.68380.uvcRRECXRM.MS', 'zen.2455819.69075.uvcRRECXRM.MS', 'zen.2455819.69771.uvcRRECXRM.MS']

	for filename in filenames:
		#extract the local sidereal time (lst) from the data
		lst = filename.strip('zen.').strip('.uvcRRECXRM.MS')
 
		#flag bad data
		lst_filetypes = ('image', 'model', 'residual', 'psf', 'flux')
		lst_names = {'image': '',
						'model': '_model',
						'residual': '_residual',
						'psf': '_psf'}

		lst_infile = {in_name: '.'.join((lst, in_name)) for in_name in lst_filetypes}
		lst_outfile = {in_name: ''.join((lst, out_name, '.fits')) for in_name, out_name in lst_names.items()}
		for in_file in lst_infile.values():
			shutil.rmtree(in_file)

		#Clear everything, and create a new table
		clearcal(vis=filename, addmodel=True)

		# make an image
		clean(vis=filename, imagename=lst, imagermode='csclean', psfmode='clark', threshold='20Jy', niter=10000, mode='mfs', weighting='uniform',
				cell=['3arcmin', '3arcmin'], imsize=[1024, 1024], gridmode='widefield', wprojplanes=32, gain=0.2, stokes='I', usescratch=True)

		#Use scratch puts the model back into the MS
		clean2fits(lst_infile, lst_outfile)

		#calibrate the gain frequency dependence
		bandpass_table = ''.join((lst, '_bandpass.cal'))
		normalized_bandpass = ''.join((lst, '_normbandpass.cal'))
		bandpass(vis=filename, solint='inf', combine='scan', caltable=bandpass_table, field='')
		bandpass_normalize(bandpass_table, normalized_bandpass)
		applycal(vis=filename, gaintable=normalized_bandpass)

		bp_image = ''.join((lst, '_bandpass'))
		bp_infile = {in_name: '.'.join((bp_image, in_name)) for in_name in lst_filetypes}
		bp_outfile = {in_name: ''.join((bp_image, out_name, '.fits')) for in_name, out_name in lst_names.items()}
		for in_file in bp_infile.values():
			shutil.rmtree(in_file)
		
		#make an image
		clean(vis=filename, imagename=out_image, imagermode='csclean', psfmode='clark', threshold='10Jy', niter=10000, mode='mfs',
				weighting='uniform', cell=['3arcmin', '3arcmin'], imsize=[1024, 1024], gridmode='widefield', wprojplanes=32, gain=0.2,
				stokes='I')

		clean2fits(bp_infile, bp_outfile)
