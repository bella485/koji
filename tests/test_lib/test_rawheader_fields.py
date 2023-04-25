# coding=utf-8
from __future__ import absolute_import
import os.path
import unittest

import koji


class TestRawHeaderFields(unittest.TestCase):

    RPMFILES = [
        "test-deps-1-1.fc24.x86_64.rpm",
        "test-files-1-1.fc27.noarch.rpm",
        "test-nosrc-1-1.fc24.nosrc.rpm",
        "test-deps-1-1.fc24.x86_64.rpm.signed",
        "test-nopatch-1-1.fc24.nosrc.rpm",
        "test-src-1-1.fc24.src.rpm",
    ]

    def test_header_sizes(self):
        for basename in self.RPMFILES:
            fn = os.path.join(os.path.dirname(__file__), 'data/rpms', basename)

            rh = koji.RawHeader(koji.rip_rpm_hdr(fn))
            hdr = koji.get_rpm_header(fn)
            for key in rh.index:
                if key in (63, 1141):
                    continue
                ours = rh.get(key, decode=True)
                if isinstance(hdr[key], bytes) and not isinstance(ours, bytes):
                    if isinstance(ours, list):
                        ours = ours[0].encode('utf-8')
                    else:
                        ours = ours.encode('utf-8')
                    theirs = hdr[key]
                else:
                    if isinstance(hdr[key], list):
                        theirs = []
                        for i in hdr[key]:
                            if isinstance(i, bytes):
                                theirs.append(i.decode('utf-8'))
                            else:
                                theirs.append(i)
                    else:
                        theirs = hdr[key]
                if type(ours) != type(theirs):
                    if isinstance(ours, list) and len(ours) == 1 and ours[0] == theirs:
                        # rpm is presenting as a scalar
                        continue
                # otherwise
                self.assertEqual(ours, theirs)
