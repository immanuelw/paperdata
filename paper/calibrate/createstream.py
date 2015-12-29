'''----------------------------------------------------------------------------
    createstream.py

        Written by: Jason Ling
        Last Modified: August 4th, 2014

        execution: 
        python createstream.py
        output: 
        converted hdf5 files from uv files
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

if __name__ == '__main__':
    #print 'Initializing program'
    # Declares and initiates variables
    directory = 'psa904/'
    #raw_data = '/data4/raw_data/Sep2011/' + directory
    raw_data = os.path.join('/data4/raw_data/psa903-930/', directory)
    home = '/data4/paper/mmodes/'
    timestream = os.path.join(home, 'storage/psa32redun/', directory, 'RREXC_timestream/')
    basepath = os.path.join(home, 'storage/psa32redun/', directory, 'RREXC_baseline/')
    #timestream = '/data2/home/lingj/anaconda/pkgs/driftscan-master/examples/paper/pydriver/ts1/timestream_f/'
    base = N.empty((528, 2), dtype=int)
    polarization = N.array(['yy','xx','xy','yx'])
    suffix = '.uvcRREXC'
    i = 0
    first = False
    time = 0
    polnum = 0
    spec_num = 0
    freq = 0

    # Reads in the uv files
    files = sorted(glob(''.join((raw_data, 'zen*', suffix)))
    num_files = len(files)

    # 3D arrays to contain the data and flag information
    #size = (528, 203, 4, num_files*14)
    #data_tab = N.empty(size, dtype=N.complex128)
    basedata = N.empty((203, 4, num_files * 14), dtype=N.complex128)
    #flags = N.zeros(size, dtype='i8')

    '''----------------------------------------------
            SECTION: Preparing the baselines
    --------------------------------------------'''

    print 'Preparing the baselines'

    # Fills the base array with the proper combinations of antenna pairs
    # Starts a new first antenna once the second one hits max. e.g.
    #      1_31 is followed by 2_0
    for baseline1 in range(32):
        for baseline2 in range(32):
            # Starts the filling with an antenna with itself. e.g.
            #      0_0 or 3_3 etc
            if baseline2 >= baseline1:
                base[i][0] = baseline1
                base[i][1] = baseline2
                i += 1
        
    '''----------------------------------------------
            SECTION: Reads in data from files
    --------------------------------------------'''
    print 'Reading in data from uv files'

    # Iterates through all of the possible baselines
    for n, (b1, b2) in enumerate(base):

        b = '{b1}_{b2}'.format(b1=b1, b2=b2)
        print b 
        first = True

        # Iterates through all of the possible polarizations
        #   Note: since we don't yet fully understand Shaw's pipeline, 
        #       polarization is not entirely utilized
            for pol in polarization:
                if pol == 'xx': 
                    polnum = 0
                elif pol == 'xy':
                    polnum = 1
                elif pol == 'yx':
                    polnum = 2
                elif pol == 'yy':
                    polnum = 3

            # Iterates through all of the available files
                for k, fl in enumerate(files):
                
                # Establishes a uv object from the file
                    uv = A.miriad.UV(fl)

                    # Selects the uv objects with the right baseline 
                    #      and polarization
                    A.scripting.uv_selector(uv, b, pol)

                    spec_num = 0

                # Iterates through the data of all uv objects of a file
                    for (uvw, t, (cor1, cor2)), d, fg in uv.all(raw=True):                              
                        freq = 0
                        #row = k*14 + spec_num
                                    
                        # Iterates through all of the frequencies
                        # Fills in data
                        for l, datum in enumerate(d):
                            #print datum.shape
                        #   spec_num = 909
                            basedata[freq][polnum][k * 14 + spec_num] = datum
                            freq += 1
                        spec_num += 1
                            #print freq
                        #print spec_num
                        #print num_files * 14
        #print spec_num
        #print freq
        # Writes the data to an intermediate hdf5 file grouped by baselines
        temp_file = ''.join((basepath, b, '.hdf5'))
        #if not os.path.exists(timestream + 'baselines/'):
        #   os.makedirs(timestream + 'baselines/')
        with h5py.File(temp_file, 'w') as f:
        # Creates the data set, gives values, and closes the file
            daset = f.create_dataset('data', (203, 4, num_files * 14), dtype=N.complex128)
            daset[...] = basedata[...]
            sys.stdout.flush()

    # Testing Code below
    '''
    file = timestream + 'baselines/timestream.hdf5'
    fl = h5py.File(file,'w')
    dset = fl.create_dataset((528,num_files*14),dtype=N.complex128)
    inname = timestream + 'baselines/0_0.hdf5'
    infile = h5py.File(inname,'r')
    indata = infile['/data'][:]
    dset[0][:] = indata[0][0][:]
    print dset[0][100]
    print indata[0][0][100]'''

    '''----------------------------------------------
        SECTION: Writes Timestream
    --------------------------------------------'''
    '''
    newfiles = glob(basepath + '*.hdf5')
    newfiles.sort()
    num_files = len(newfiles)

    base1 = 0
    base2 = 0
    freq = 0

    # Obtains the dimensions of the timestream that will be used
    example = h5py.File(newfiles[0],'r')
    dimen = example['/data'][:].shape # (203, 4, 1988)
    example.close()
    stream = N.empty((num_files,dimen[2]),dtype=N.complex128)

    # Loop that iterates through all frequencies and sets up timestream
    while True:
        # Check for correct frequency
        if freq >= dimen[0]:
            break
        print freq
        sys.stdout.flush()
        freqpath = timestream + str(freq) + '/'

        # Iterates through all of the baseline hdf5 files made in createstream.py
        for k,fl in enumerate(newfiles):

            # Opens file to read
            file = h5py.File(fl,'r')
            indata = file['/data'][:]

            # Isolates the correct frequency in the opened read file
            i = 0
            for fre in indata:
                if i != freq:
                    i += 1
                    continue
                for pol in fre:
                    # Writes the time visibilities to an output array
                    for ti in pol:
                        stream[k][ti] = indata[freq][0][ti]
                    break
                break
            file.close()

        # Makes the frequency directories
        if not os.path.exists(freqpath):
            os.makedirs(freqpath)

        # Prepares the output file
        out = h5py.File(freqpath + 'timestream.hdf5','a')
        dset = out.create_dataset('timestream', (num_files,dimen[2]),dtype=N.complex128)
        dset[...] = stream[...]
        out.close()

        # Iterate until done
        freq += 1
    '''


    # This entire section was found not to work. See basetofreq.py for the 
    #   second part of the uv to hdf5 conversion

    '''freq = 0
    while True:
        if freq > 202:
            break
        print 'Timestream of ' + str(freq) + ' frequency'
        # Prepares the hdf5 file
        file = timestream + str(freq) + '/timestream.hdf5'
        if not os.path.exists(timestream + str(freq) + '/'):
            os.makedirs(timestream + str(freq) + '/')
        f = h5py.File(file, 'w')

        # Declares the datasets to be used
            dset = f.create_dataset('timestream', (528, num_files*14), dtype=N.complex64)
            fset = f.create_dataset('flags', (528, num_files*14), dtype='i8')

        tstreambase = 0
        tstream = 0
        base1 = 0
        base2 = 0
        while True:
            if base1 > 31:
                break
            base2 = base1
            for i in xrange(base2,32):
                for m,[ba1,ba2] in enumerate(base):
                    if base1 != ba1:
                        continue
                    if base2 != ba2:
                        continue
                    inname = timestream+'baselines/'+str(ba1)+'_'+str(ba2)+'.hdf5'
                    infile = h5py.File(inname,'r')
                    indata = infile['/data'][:]
                    dset[tstreambase][:] = indata[freq][0][:]
                    fset[tstreambase][...] = flags[freq][m][0][:]
                tstreambase += 1
                base2 += 1
            base1 += 1
            # Adds the start time as an attribute to the dataset
            #dset.attrs['time'] = time
            # Closes the file
            f.close()
        
        freq += 1'''
