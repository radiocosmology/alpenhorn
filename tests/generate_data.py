"""Generates a sub set of CHIME data for testing alpenhorn's archiving.

"""

import os
import shutil
from os import path
import glob

import numpy as np

from ch_util import andata

ARCHIVE_ROOT = '/scratch/k/krs/jrs65/chime/archive/online/'

OUTPUT_DIR = './testdata/'

CORR_ACQS = [
    '20150709T011642Z_pathfinder_corr',
    '20160311T212816Z_pathfinder_corr',
    '20160715T202149Z_pathfinder_corr'
]

OTHER_ACQS = ['20121207T174000Z_mingun_weather']

NCORR = 4
NOTHER = 3

FREQ_SEL = np.s_[64:66]
INPUT_SEL = np.s_[0:5]

START = 0
STOP = 5


def main():
    # Open data files and cast as andata objects.

    for acq in CORR_ACQS:
        print "Processing acq=%s" % acq

        acqdir = path.join(ARCHIVE_ROOT, acq)
        outdir = path.join(OUTPUT_DIR, acq)

        # Make the new acquisition directory
        os.makedirs(outdir)

        # Copy over the log files (first 20 lines)
        if path.exists(path.join(acqdir, 'ch_master.log')):

            with open(path.join(acqdir, 'ch_master.log'), mode='r') as log_in:
                with open(path.join(outdir, 'ch_master.log'), mode='w+') as log_out:

                    for li, line in enumerate(log_in):
                        if li > 20:
                            break
                        log_out.write(line)

        # Open and copy correlator files, but trim down the datasets so they are
        # more manageable sizes (also zero out the data)
        files = sorted(glob.glob(acqdir + '/*.h5'))[:NCORR]

        for fname in files:
            ad = andata.CorrData.from_acq_h5(fname, start=START, stop=STOP,
                                             input_sel=INPUT_SEL, freq_sel=FREQ_SEL)

            ad.vis[:] = 0.0
            ad.save(path.join(outdir, path.basename(fname)))

    # Iterate over other acquisition types and copy a number of files over
    for acq in OTHER_ACQS:
        print "Processing acq=%s" % acq

        acqdir = path.join(ARCHIVE_ROOT, '..', 'staging', acq)
        outdir = path.join(OUTPUT_DIR, acq)

        # Make the new acquisition directory
        os.makedirs(outdir)

        for fname in sorted(glob.glob(acqdir + '/*'))[:NOTHER]:
            outfile = path.join(outdir, path.basename(fname))
            shutil.copy(fname, outfile)

if __name__ == '__main__':
    main()
