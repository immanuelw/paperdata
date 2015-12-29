import numpy as np
import h5py

from drift.core import manager#, beamtransfer
import beamtransfer
#from drift.pipeline 
#from drift.pipeline 
import timestream
import sys
from ideal32array import PaperArray

if __name__ == '__main__':
    ### Make the analysis products for the telescope. This examples focuses only
    ### on the m-mode products for mapmaking

    # Create telescope object and set zenith
    tel = PaperArray()
    tel.zenith = np.radians(np.array([90 + 30.72152778, 21.42830556]))  # Must be in radians

    print 'Looking at beam transfer matricies'
    sys.stdout.flush()
    # Create Beam Transfer manager, and generate products
    bt = beamtransfer.BeamTransfer('pydriver/btdir/',  telescope=tel)
    bt.generate()

    ### Simulate and make a map froma timestream

    # Create an empty ProductManager
    m = manager.ProductManager()

    # Set the Beam Transfers
    m.beamtransfer = bt

    print 'Looking at timestreams'
    sys.stdout.flush()
    # Create a timestream with no noise (ndays=0) from a given map (could be a list of maps)
    ts = timestream.simulate(m, 'pydriver/ts1/', ['simulated_map.hdf5'], ndays=0)
    # Make m-mode from the timestream
    ts.generate_mmodes()

    print 'Making map'
    sys.stdout.flush()
    # Make a Healpix map from the m-modes (with NSIDE=256)
    ts.mapmake_full(256, 'observed_map.hdf5')
