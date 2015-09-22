def clean2fits(imagename,outname):
  images = ['image','residual','model','psf']
  for image in images:
	ia.open(imagename, '.', image)
	ia.tofits(outname, '_', image, '.fits',overwrite=True)
	ia.close()
  return True

if __name__ == '__main__':
	filename = 'zen.2455819.51677.uvcRRECXRM.MS.copy1'
	filename_raw = 'zen.2455819.51677.uvcRRECXRM.MS'

	# Make the source model a separate function (it doesn't need to know
	# about the measurement set, just the sky
	if False:
		# Build the source model
		#03:24:05.5, -37:17:09
		#03:21:38.0, -37:08:52
		fornaxAi =  'J2000 03h24m07.9s -37d16m25s'
		fornaxAii = 'J2000 03h21m37.9s -37d08m51s'
		cl.done()
		cl.addcomponent(dir=fornaxAi, flux=43, fluxunit='Jy', freq='0.150GHz', shape='Gaussian',
						majoraxis='0.1arcmin', minoraxis='0.1arcmin', positionangle='0deg')
		cl.addcomponent(dir=fornaxAii, flux=86, fluxunit='Jy', freq='0.150GHz', shape='Gaussian',
						majoraxis='0.1arcmin', minoraxis='0.1arcmin', positionangle='0deg')
		cl.rename('FornaxA.cl')
		cl.close()

	# Add the source model to the visibilities
	ft(vis=filename, complist='FornaxA.cl', usescratch=True)

	bandpass_table= ''.join(filename, '_bandpass.cal')
	bandpass(vis=filename, solint='inf', combine='scan', caltable=bandpass_table, field='')
	applycal(vis=filename, gaintable=bandpass_table)

	cal_image = ''.join(filename, '.cal')

	clean(vis=filename, imagename=cal_image, imagermode='csclean', psfmode='clark', threshold='10Jy', niter=10000, mode='mfs', weighting='uniform', cell=['3arcmin', '3arcmin'], imsize=[1024, 1024], gridmode='widefield', wprojplanes=32, gain=0.2, stokes='I')

	clean2fits(cal_image, cal_image)

	raw_image = filename_raw

	fixed_raw = ''.join(filename_raw, '_image')

	clean(vis=filename_raw, imagename=fixed_raw, imagermode='csclean', psfmode='clark', threshold='10Jy', niter=10000, mode='mfs', weighting='uniform', cell=['3arcmin', '3arcmin'], imsize=[1024, 1024], gridmode='widefield', wprojplanes=32, gain=0.2, stokes='I')

	clean2fits(fixed_raw, fixed_raw)
