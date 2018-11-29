from __future__ import absolute_import
import mock
try:
    import unittest2 as unittest
except ImportError:
    import unittest

import kojihub
import os
import random
from tempfile import gettempdir


def get_temp_dir_root():
    return os.path.join(gettempdir(), 'koji_tests')


def get_tmp_dir_path(folder_starts_with):
    return os.path.join(get_temp_dir_root(), ('{0}{1}'.format(folder_starts_with, random.randint(1, 999999999999))))


class TestListFiles(unittest.TestCase):
    def setUp(self):
        self.hub = kojihub.RootExports()
        self.standard_processor_kwargs = dict(
            tables=mock.ANY,
            columns=mock.ANY,
            values=mock.ANY,
            joins=mock.ANY,
            clauses=mock.ANY,
            opts=mock.ANY,
            aliases=mock.ANY,
        )

    @mock.patch('koji.pathinfo.work')
    def test_root_exports_listFiles(self, koji_pathinfo_work):
        temp_path = get_tmp_dir_path('TestTask')
        koji_pathinfo_work.return_value = temp_path
        structure_to_create = {
            'subdirs': {
                'dir1': {
                    'subdirs': {
                        'dir2': {
                            'subdirs': {},
                            'files': {
                                'file3': {'size': 30},
                                'file4': {'size': 7}
                            }
                        },
                        'dir3': {
                            'subdirs': {},
                            'files': {
                                'file5': {'size': 17},
                                'file6': {'size': 23}
                            }
                        }
                    },
                    'files': {
                        'file1': {'size': 20},
                        'file2': {'size': 10}
                    }
                }
            },
            'files': {}
        }
        test_structure_2 = {
            'subdirs': {
                'dir1': {
                    'subdirs': {
                        'dir2': {
                            'subdirs': {},
                            'files': {
                                'file3': {'size': 30},
                                'file4': {'size': 7}
                            }
                        }
                    },
                    'files': {
                        'file2': {'size': 10}
                    }
                }
            },
            'files': {}
        }
        self._create_dir_structure(temp_path, structure_to_create)

        self.assertDictEqual(self.hub.listFiles('dir1/*'), structure_to_create)
        self.assertDictEqual(self.hub.listFiles('dir1/*2'), test_structure_2)

    def _create_dir_structure(self, current_dir, dir_struct):
        os.makedirs(current_dir)
        for k in dir_struct.get('subdirs', {}).keys():
            sub_path = os.path.join(current_dir, k)
            self._create_dir_structure(sub_path, dir_struct['subdirs'][k])
        for k in dir_struct.get('files', {}).keys():
            sub_path = os.path.join(current_dir, k)
            with open(sub_path, 'w+b') as new_file:
                with open('/dev/urandom') as urand_file:
                    new_file.write(urand_file.read(dir_struct['files'][k]['size']))

