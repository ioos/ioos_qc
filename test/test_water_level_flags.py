#!/usr/bin/env python
'''
test.test_wind_flags

Tests for Flag Setting and Checking for Winds
'''

from ioos_qartod.flags import QARTODEval
from ioos_qartod.flags.water_level import *

import numpy as np
import unittest

class TestWindFlags(unittest.TestCase):

    def setUp(self):
        self.flags = QARTODEval()

    def test_timing_gap(self):
        '''
        Tests the TimingGap Test Flags
        '''

        self.flags.mark_test(TimingGap.passes())

        assert self.flags.check(TimingGap) == TimingGap.pass_flag
        assert self.flags.check_str(TimingGap) == 'pass_flag'
        
        self.flags.mark_test(TimingGap.fail())
        
        assert self.flags.check(TimingGap) == TimingGap.fail_flag
        assert self.flags.check_str(TimingGap) == 'fail_flag'

        self.flags.mark_test(TimingGap.not_eval())
        
        assert self.flags.check(TimingGap) == TimingGap.not_eval_flag
        assert self.flags.check_str(TimingGap) == 'not_eval_flag'

    def test_syntax_check(self):
        '''
        Tests the SyntaxCheck Test Flags
        '''

        self.flags.mark_test(SyntaxCheck.passes())

        assert self.flags.check(SyntaxCheck) == SyntaxCheck.pass_flag
        assert self.flags.check_str(SyntaxCheck) == 'pass_flag'
        
        self.flags.mark_test(SyntaxCheck.fail())
        
        assert self.flags.check(SyntaxCheck) == SyntaxCheck.fail_flag
        assert self.flags.check_str(SyntaxCheck) == 'fail_flag'

        self.flags.mark_test(SyntaxCheck.not_eval())
        
        assert self.flags.check(SyntaxCheck) == SyntaxCheck.not_eval_flag
        assert self.flags.check_str(SyntaxCheck) == 'not_eval_flag'
    
    def test_location_check(self):
        '''
        Tests the LocationTest Test Flags
        '''

        self.flags.mark_test(LocationTest.passes())

        assert self.flags.check(LocationTest) == LocationTest.pass_flag
        assert self.flags.check_str(LocationTest) == 'pass_flag'
        
        self.flags.mark_test(LocationTest.fail())
        
        assert self.flags.check(LocationTest) == LocationTest.fail_flag
        assert self.flags.check_str(LocationTest) == 'fail_flag'

        self.flags.mark_test(LocationTest.not_eval())
        
        assert self.flags.check(LocationTest) == LocationTest.not_eval_flag
        assert self.flags.check_str(LocationTest) == 'not_eval_flag'
        
        self.flags.mark_test(LocationTest.suspect())
        
        assert self.flags.check(LocationTest) == LocationTest.suspect_flag
        assert self.flags.check_str(LocationTest) == 'suspect_flag'
    
    def test_gross_range(self):
        '''
        Tests the GrossRangeTest Test Flags
        '''

        self.flags.mark_test(GrossRangeTest.passes())

        assert self.flags.check(GrossRangeTest) == GrossRangeTest.pass_flag
        assert self.flags.check_str(GrossRangeTest) == 'pass_flag'
        
        self.flags.mark_test(GrossRangeTest.fail())
        
        assert self.flags.check(GrossRangeTest) == GrossRangeTest.fail_flag
        assert self.flags.check_str(GrossRangeTest) == 'fail_flag'

        self.flags.mark_test(GrossRangeTest.not_eval())
        
        assert self.flags.check(GrossRangeTest) == GrossRangeTest.not_eval_flag
        assert self.flags.check_str(GrossRangeTest) == 'not_eval_flag'
        
        self.flags.mark_test(GrossRangeTest.suspect())
        
        assert self.flags.check(GrossRangeTest) == GrossRangeTest.suspect_flag
        assert self.flags.check_str(GrossRangeTest) == 'suspect_flag'
    
    def test_climatology(self):
        '''
        Tests the ClimatologyTest Test Flags
        '''

        self.flags.mark_test(ClimatologyTest.passes())

        assert self.flags.check(ClimatologyTest) == ClimatologyTest.pass_flag
        assert self.flags.check_str(ClimatologyTest) == 'pass_flag'
        
        self.flags.mark_test(ClimatologyTest.fail())
        
        assert self.flags.check(ClimatologyTest) == ClimatologyTest.fail_flag
        assert self.flags.check_str(ClimatologyTest) == 'fail_flag'

        self.flags.mark_test(ClimatologyTest.not_eval())
        
        assert self.flags.check(ClimatologyTest) == ClimatologyTest.not_eval_flag
        assert self.flags.check_str(ClimatologyTest) == 'not_eval_flag'
        
        self.flags.mark_test(ClimatologyTest.suspect())
        
        assert self.flags.check(ClimatologyTest) == ClimatologyTest.suspect_flag
        assert self.flags.check_str(ClimatologyTest) == 'suspect_flag'
    
    def test_spike(self):
        '''
        Tests the SpikeTest Test Flags
        '''

        self.flags.mark_test(SpikeTest.passes())

        assert self.flags.check(SpikeTest) == SpikeTest.pass_flag
        assert self.flags.check_str(SpikeTest) == 'pass_flag'
        
        self.flags.mark_test(SpikeTest.fail())
        
        assert self.flags.check(SpikeTest) == SpikeTest.fail_flag
        assert self.flags.check_str(SpikeTest) == 'fail_flag'

        self.flags.mark_test(SpikeTest.not_eval())
        
        assert self.flags.check(SpikeTest) == SpikeTest.not_eval_flag
        assert self.flags.check_str(SpikeTest) == 'not_eval_flag'
        
        self.flags.mark_test(SpikeTest.suspect())
        
        assert self.flags.check(SpikeTest) == SpikeTest.suspect_flag
        assert self.flags.check_str(SpikeTest) == 'suspect_flag'
    
    def test_rate_of_change(self):
        '''
        Tests the RateOfChangeTest Test Flags
        '''

        self.flags.mark_test(RateOfChangeTest.passes())

        assert self.flags.check(RateOfChangeTest) == RateOfChangeTest.pass_flag
        assert self.flags.check_str(RateOfChangeTest) == 'pass_flag'
        
        self.flags.mark_test(RateOfChangeTest.not_eval())
        
        assert self.flags.check(RateOfChangeTest) == RateOfChangeTest.not_eval_flag
        assert self.flags.check_str(RateOfChangeTest) == 'not_eval_flag'
        
        self.flags.mark_test(RateOfChangeTest.suspect())
        
        assert self.flags.check(RateOfChangeTest) == RateOfChangeTest.suspect_flag
        assert self.flags.check_str(RateOfChangeTest) == 'suspect_flag'
    
    def test_flatline(self):
        '''
        Tests the FlatLineTest Test Flags
        '''

        self.flags.mark_test(FlatLineTest.passes())

        assert self.flags.check(FlatLineTest) == FlatLineTest.pass_flag
        assert self.flags.check_str(FlatLineTest) == 'pass_flag'
        
        self.flags.mark_test(FlatLineTest.fail())
        
        assert self.flags.check(FlatLineTest) == FlatLineTest.fail_flag
        assert self.flags.check_str(FlatLineTest) == 'fail_flag'

        self.flags.mark_test(FlatLineTest.not_eval())
        
        assert self.flags.check(FlatLineTest) == FlatLineTest.not_eval_flag
        assert self.flags.check_str(FlatLineTest) == 'not_eval_flag'
        
        self.flags.mark_test(FlatLineTest.suspect())
        
        assert self.flags.check(FlatLineTest) == FlatLineTest.suspect_flag
        assert self.flags.check_str(FlatLineTest) == 'suspect_flag'
    
    def test_multivariate(self):
        '''
        Tests the MultivariateTest Test Flags
        '''

        self.flags.mark_test(MultivariateTest.passes())

        assert self.flags.check(MultivariateTest) == MultivariateTest.pass_flag
        assert self.flags.check_str(MultivariateTest) == 'pass_flag'

        self.flags.mark_test(MultivariateTest.not_eval())
        
        assert self.flags.check(MultivariateTest) == MultivariateTest.not_eval_flag
        assert self.flags.check_str(MultivariateTest) == 'not_eval_flag'
        
        self.flags.mark_test(MultivariateTest.suspect())
        
        assert self.flags.check(MultivariateTest) == MultivariateTest.suspect_flag
        assert self.flags.check_str(MultivariateTest) == 'suspect_flag'
    
    def test_attenuated_signal(self):
        '''
        Tests the AttenuatedSignalTest Test Flags
        '''

        self.flags.mark_test(AttenuatedSignalTest.passes())

        assert self.flags.check(AttenuatedSignalTest) == AttenuatedSignalTest.pass_flag
        assert self.flags.check_str(AttenuatedSignalTest) == 'pass_flag'
        
        self.flags.mark_test(AttenuatedSignalTest.fail())
        
        assert self.flags.check(AttenuatedSignalTest) == AttenuatedSignalTest.fail_flag
        assert self.flags.check_str(AttenuatedSignalTest) == 'fail_flag'

        self.flags.mark_test(AttenuatedSignalTest.not_eval())
        
        assert self.flags.check(AttenuatedSignalTest) == AttenuatedSignalTest.not_eval_flag
        assert self.flags.check_str(AttenuatedSignalTest) == 'not_eval_flag'
        
        self.flags.mark_test(AttenuatedSignalTest.suspect())
        
        assert self.flags.check(AttenuatedSignalTest) == AttenuatedSignalTest.suspect_flag
        assert self.flags.check_str(AttenuatedSignalTest) == 'suspect_flag'
    
    def test_neighbor(self):
        '''
        Tests the NeighborTest Test Flags
        '''

        self.flags.mark_test(NeighborTest.passes())

        assert self.flags.check(NeighborTest) == NeighborTest.pass_flag
        assert self.flags.check_str(NeighborTest) == 'pass_flag'

        self.flags.mark_test(NeighborTest.not_eval())
        
        assert self.flags.check(NeighborTest) == NeighborTest.not_eval_flag
        assert self.flags.check_str(NeighborTest) == 'not_eval_flag'
        
        self.flags.mark_test(NeighborTest.suspect())
        
        assert self.flags.check(NeighborTest) == NeighborTest.suspect_flag
        assert self.flags.check_str(NeighborTest) == 'suspect_flag'
    
    def test_aggregate(self):
        '''
        Tests the AggregateTest Test Flags
        '''

        self.flags.mark_test(AggregateTest.passes())

        assert self.flags.check(AggregateTest) == AggregateTest.pass_flag
        assert self.flags.check_str(AggregateTest) == 'pass_flag'
        
        self.flags.mark_test(AggregateTest.fail())
        
        assert self.flags.check(AggregateTest) == AggregateTest.fail_flag
        assert self.flags.check_str(AggregateTest) == 'fail_flag'

        self.flags.mark_test(AggregateTest.not_eval())
        
        assert self.flags.check(AggregateTest) == AggregateTest.not_eval_flag
        assert self.flags.check_str(AggregateTest) == 'not_eval_flag'
        
        self.flags.mark_test(AggregateTest.suspect())
        
        assert self.flags.check(AggregateTest) == AggregateTest.suspect_flag
        assert self.flags.check_str(AggregateTest) == 'suspect_flag'

    def test_numpy(self):
        '''
        Tests the flags for numpy support
        '''

        flags = np.zeros(20, dtype='uint32')
        self.flags = QARTODEval(flags)
        self.flags.mark_test(NeighborTest.passes())

        expected = np.ones(20, dtype='uint32') * NeighborTest.pass_flag
        result = self.flags.check(NeighborTest)
        np.testing.assert_array_equal(expected, result)

        expected = ['pass_flag' for i in xrange(20) ]
        result = self.flags.check_str(NeighborTest)
        assert expected == result

