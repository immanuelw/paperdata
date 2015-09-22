
import sys,os
import cmath
from casa import table as tb
from casa import image as ia
from casa import flagdata, clean
from casa import *
import numpy as np
#import pyfits as pf

def bandpass_normalize(bandpass_table,bandpass_table_inv):
  cmd = 'cp -r ' + bandpass_table + ' ' + bandpass_table_inv
  os.system(cmd)
  tb.open(bandpass_table_inv,nomodify=False)
  gain=tb.getcol('CPARAM')
  gain_norm=gain
# 
# for each antenna and for each channel of the bandpass I divide out by the modulo of the complex number
#  
  for i in range(0,gain.shape[2]):
   for j in range(0,gain.shape[1]):
    a = gain[0,j,i]
#
# if the real part of the antenna gain is set to 1 it means that that antenna a/o channel is flag, so don't bother looking at it
#   
    if a.real != 1:
     gain_norm[0,j,i] = gain_norm[0,j,i]/abs(gain_norm[0,j,i]) 
     gain_norm[1,j,i] = gain_norm[1,j,i]/abs(gain_norm[1,j,i])
#     print abs(gain_norm[i,j,0]),abs(gain[i,j,0]),i,j
#
#
# put back the normalized bandpass
#    
  tb.putcol('CPARAM',gain_norm)
  tb.close()
  tb.done()

#
# look for there files on elwood
#
filenames = ['zen.2455819.50285.uvcRRECXRM.MS']
#,'zen.2455819.50981.uvcRRECXRM.MS','zen.2455819.51677.uvcRRECXRM.MS','zen.2455819.52373.uvcRRECXRM.MS','zen.2455819.53069.uvcRRECXRM.MS','zen.2455819.53765.uvcRRECXRM.MS','zen.2455819.54461.uvcRRECXRM.MS','zen.2455819.55157.uvcRRECXRM.MS','zen.2455819.55853.uvcRRECXRM.MS','zen.2455819.56548.uvcRRECXRM.MS','zen.2455819.57244.uvcRRECXRM.MS','zen.2455819.57940.uvcRRECXRM.MS','zen.2455819.58636.uvcRRECXRM.MS','zen.2455819.59332.uvcRRECXRM.MS','zen.2455819.60028.uvcRRECXRM.MS','zen.2455819.60724.uvcRRECXRM.MS','zen.2455819.61420.uvcRRECXRM.MS','zen.2455819.62116.uvcRRECXRM.MS','zen.2455819.62812.uvcRRECXRM.MS','zen.2455819.63508.uvcRRECXRM.MS','zen.2455819.64204.uvcRRECXRM.MS','zen.2455819.64900.uvcRRECXRM.MS','zen.2455819.65596.uvcRRECXRM.MS','zen.2455819.66292.uvcRRECXRM.MS','zen.2455819.66988.uvcRRECXRM.MS','zen.2455819.67684.uvcRRECXRM.MS','zen.2455819.68380.uvcRRECXRM.MS','zen.2455819.69075.uvcRRECXRM.MS','zen.2455819.69771.uvcRRECXRM.MS']

#
#for i in range(0,len(filenames)):
#
filename = filenames[0]
a = filename.strip('zen.')
lst = a.strip('.uvcRRECXRM.MS')
# extract the local sidereal time (lst) from the data
#
 
#
# flag bad data
#
out_fits  = lst+'.fits'
out_fits_res  = lst+'_residual.fits'
out_fits_model  = lst+'_model.fits'
out_fits_psf = lst+'_psf.fits'
#
cmd = 'rm -rf '+ lst + '.image ' + lst + '.model ' + lst + '.residual ' + lst + '.flux' + lst + '.psf'
os.system(cmd)
# Clear everything, and create a new table
clearcal(vis=filename,addmodel=True)
# make an image
clean(vis=filename,imagename=lst,imagermode='csclean',psfmode='clark',threshold='20Jy',niter=10000,mode='mfs',weighting='uniform',cell=['3arcmin','3arcmin'],imsize=[1024,1024],gridmode='widefield',wprojplanes=32,gain=0.2,stokes='I',usescratch=True)
# Use scratch puts the model back into the MS
ia.open(lst+'.image')
ia.tofits(out_fits,overwrite=True)
ia.close()
ia.open(lst+'.residual')
ia.tofits(out_fits_res,overwrite=True)
ia.close()
ia.open(lst+'.model')
ia.tofits(out_fits_model,overwrite=True)
ia.close()
ia.open(lst+'.psf')
ia.tofits(out_fits_psf,overwrite=True)
ia.close()

# calibrate the gain frequency dependence
bandpass_table = lst+'_bandpass.cal'
normalized_bandpass = lst+'_normbandpass.cal'
bandpass(vis=filename,solint='inf',combine='scan',caltable=bandpass_table,field='')
bandpass_normalize(bandpass_table,normalized_bandpass)
applycal(vis=filename,gaintable=normalized_bandpass)

out_image = lst+'_bandpass'
out_fits  = out_image+'.fits'
out_fits_res  = out_image+'_residual.fits'
out_fits_model  = out_image+'_model.fits'
out_fits_psf = out_image+'_psf.fits'
#
cmd = 'rm -rf '+ out_image + '.image ' + out_image + '.model ' + out_image + '.residual ' + out_image + '.flux' + out_image + '.psf'
os.system(cmd)
#
# make an image
#
clean(vis=filename,imagename=out_image,imagermode='csclean',psfmode='clark',threshold='10Jy',niter=10000,mode='mfs',weighting='uniform',cell=['3arcmin','3arcmin'],imsize=[1024,1024],gridmode='widefield',wprojplanes=32,gain=0.2,stokes='I')
ia.open(out_image+'.image')
ia.tofits(out_fits,overwrite=True)
ia.close()
ia.open(out_image+'.residual')
ia.tofits(out_fits_res,overwrite=True)
ia.close()
ia.open(out_image+'.model')
ia.tofits(out_fits_model,overwrite=True)
ia.close()
ia.open(out_image+'.psf')
ia.tofits(out_fits_psf,overwrite=True)
ia.close()

def clean2fits(imagename,outname):
  images = ['image','residual','model','psf']
  for image in images:
    ia.open(imagename+'.'+image)
    ia.tofits(outname+'_'+image+'.fits',overwrite=True)
    ia.close()
  return True
