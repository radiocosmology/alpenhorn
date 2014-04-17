#!/usr/bin/env python
import os

from termcolor import colored
from progress.bar import Bar

from ch_util import data_index as di


this_node = di.StorageNode.get(di.StorageNode.name == 'scinet_scratch')

## Use a complicated query with a tuples construct to fetch everything we need
## in a single query. This massively speeds up the whole process versus
## fetching all the FileCopy's then querying for Files and Acqs.
lfiles = di.ArchiveFile.select(di.ArchiveFile.name, di.ArchiveAcq.name, di.ArchiveFile.size_b).join(di.ArchiveAcq).switch(di.ArchiveFile).join(di.ArchiveFileCopy).where(di.ArchiveFileCopy.node == this_node).tuples()
nfiles = lfiles.count()

nfail = 0
print "*** Checking %i files ***" % nfiles

for filename, acqname, filesize in Bar('Verifying', max=nfiles).iter(lfiles):


    filepath = this_node.root + '/' + acqname + '/' + filename

    if not os.path.exists(filepath):
        print colored('FAIL', 'red') + ' missing file: %s' % filepath
        nfail += 1
        continue

    if os.path.getsize(filepath) != filesize:
        print colored('FAIL', 'red') + ' wrong size: %s' % filepath
        nfail += 1
        continue

print
print "*** Complete ***"
print "   Checked %i files" % nfiles
print "   %i failures" % nfail


