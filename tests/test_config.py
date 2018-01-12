#!/usr/bin/env python
# coding=utf-8
import os
import unittest
import tempfile
from pathlib import Path

import numpy as np
import numpy.testing as npt

from ruamel import yaml

from ioos_qc.config import QcConfig


class ConfigLoadTest(unittest.TestCase):

    def setUp(self):
        template = """
        qartod:
            gross_range_test:
                suspect_span: [1, 11]
                fail_span:
                    - 0
                    - 12
            goober:
                foo: [1, null]
        """
        self.handle, self.yamlfile = tempfile.mkstemp(suffix='.yaml')
        with open(self.yamlfile, 'w') as f:
            f.write(template)

        self.expected_dict = {
            'qartod': {
                'gross_range_test': {
                    'suspect_span': [1, 11],
                    'fail_span': [0, 12],
                },
                'goober': {
                    'foo': [1, None]
                }
            }
        }

    def tearDown(self):
        os.close(self.handle)
        os.remove(self.yamlfile)

    def test_load_yaml_dict_object(self):
        with open(self.yamlfile) as f:
            y = yaml.load(f.read(), Loader=yaml.Loader)
            qc = QcConfig(y)
        assert qc.config == self.expected_dict

    def test_load_file_path(self):
        qc = QcConfig(self.yamlfile)
        assert qc.config == self.expected_dict

    def test_load_path_object(self):
        qc = QcConfig(Path(self.yamlfile))
        assert qc.config == self.expected_dict

    def test_run(self):
        qc = QcConfig(Path(self.yamlfile))
        r = qc.run(
            list(range(13))
        )

        expected = np.array([3, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3])
        npt.assert_array_equal(
            r['qartod']['gross_range_test'],
            expected
        )
