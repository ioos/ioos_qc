'''
test.test_waves

Tests for Flag Setting and Checking for Waves
'''

from ioos_qartod.flags import QARTODEval
from ioos_qartod.flags.waves import *

import numpy as np
import unittest

class TestWaveFlags(unittest.TestCase):
    def setUp(self):
        self.flags = QARTODEval()

    def test_all(self):
        for test_obj in AvailableTests:
            print 'Checking', test_obj.__name__
            self.check(test_obj)

    def check(self, test_obj):
        self.flags.mark_test(test_obj.passes())

        assert self.flags.check(test_obj) == test_obj.pass_flag
        assert self.flags.check_str(test_obj) == 'pass_flag'

        self.flags.mark_test(test_obj.not_eval())

        assert self.flags.check(test_obj) == test_obj.not_eval_flag
        assert self.flags.check_str(test_obj) == 'not_eval_flag'

        if hasattr(test_obj, 'suspect_flag'):
            self.flags.mark_test(test_obj.suspect())
            
            assert self.flags.check(test_obj) == test_obj.suspect_flag
            assert self.flags.check_str(test_obj) == 'suspect_flag'

        if hasattr(test_obj, 'fail_flag'):
            self.flags.mark_test(test_obj.fail())
            
            assert self.flags.check(test_obj) == test_obj.fail_flag
            assert self.flags.check_str(test_obj) == 'fail_flag'

