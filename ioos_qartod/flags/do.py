#!/usr/bin/env python
'''
ioos_qartod.flags.do

Flag Definitions for Dissolved Oxygen
'''

class TimingGap:
    offset        = 0
    fail_flag     = 0x0
    not_eval_flag = 0x1
    pass_flag     = 0x3

    @classmethod
    def fail(cls):
        return cls.fail_flag, cls.offset
    
    @classmethod
    def not_eval(cls):
        return cls.not_eval_flag, cls.offset
    
    @classmethod
    def passes(cls):
        return cls.pass_flag, cls.offset

class SyntaxCheck:
    offset        = 3
    fail_flag     = 0x0
    not_eval_flag = 0x1
    pass_flag     = 0x3
    
    @classmethod
    def fail(cls):
        return cls.fail_flag, cls.offset
    
    @classmethod
    def not_eval(cls):
        return cls.not_eval_flag, cls.offset
    
    @classmethod
    def passes(cls):
        return cls.pass_flag, cls.offset

class GrossRangeTest:
    offset        = 9
    fail_flag     = 0x0
    not_eval_flag = 0x1
    suspect_flag  = 0x2
    pass_flag     = 0x3
    
    @classmethod
    def fail(cls):
        return cls.fail_flag, cls.offset

    @classmethod
    def suspect(cls):
        return cls.suspect_flag, cls.offset
    
    @classmethod
    def not_eval(cls):
        return cls.not_eval_flag, cls.offset
    
    @classmethod
    def passes(cls):
        return cls.pass_flag, cls.offset

class ClimatologyTest:
    offset        = 12
    fail_flag     = 0x0
    not_eval_flag = 0x1
    suspect_flag  = 0x2
    pass_flag     = 0x3
    
    @classmethod
    def fail(cls):
        return cls.fail_flag, cls.offset

    @classmethod
    def suspect(cls):
        return cls.suspect_flag, cls.offset
    
    @classmethod
    def not_eval(cls):
        return cls.not_eval_flag, cls.offset
    
    @classmethod
    def passes(cls):
        return cls.pass_flag, cls.offset

class SpikeTest:
    offset        = 15
    fail_flag     = 0x0
    not_eval_flag = 0x1
    suspect_flag  = 0x2
    pass_flag     = 0x3
    
    @classmethod
    def fail(cls):
        return cls.fail_flag, cls.offset

    @classmethod
    def suspect(cls):
        return cls.suspect_flag, cls.offset
    
    @classmethod
    def not_eval(cls):
        return cls.not_eval_flag, cls.offset
    
    @classmethod
    def passes(cls):
        return cls.pass_flag, cls.offset

class RateOfChangeTest:
    offset        = 18
    not_eval_flag = 0x1
    suspect_flag  = 0x2
    pass_flag     = 0x3
    
    @classmethod
    def fail(cls):
        return cls.fail_flag, cls.offset

    @classmethod
    def suspect(cls):
        return cls.suspect_flag, cls.offset
    
    @classmethod
    def not_eval(cls):
        return cls.not_eval_flag, cls.offset
    
    @classmethod
    def passes(cls):
        return cls.pass_flag, cls.offset

class FlatLineTest:
    offset        = 21
    fail_flag     = 0x0
    not_eval_flag = 0x1
    suspect_flag  = 0x2
    pass_flag     = 0x3
    
    @classmethod
    def fail(cls):
        return cls.fail_flag, cls.offset

    @classmethod
    def suspect(cls):
        return cls.suspect_flag, cls.offset
    
    @classmethod
    def not_eval(cls):
        return cls.not_eval_flag, cls.offset
    
    @classmethod
    def passes(cls):
        return cls.pass_flag, cls.offset

class MultivariateTest:
    offset        = 24
    not_eval_flag = 0x1
    suspect_flag  = 0x2
    pass_flag     = 0x3
    
    @classmethod
    def suspect(cls):
        return cls.suspect_flag, cls.offset
    
    @classmethod
    def not_eval(cls):
        return cls.not_eval_flag, cls.offset
    
    @classmethod
    def passes(cls):
        return cls.pass_flag, cls.offset

class AttenuatedSignalTest:
    offset        = 27
    fail_flag     = 0x0
    not_eval_flag = 0x1
    suspect_flag  = 0x2
    pass_flag     = 0x3
    
    @classmethod
    def fail(cls):
        return cls.fail_flag, cls.offset

    @classmethod
    def suspect(cls):
        return cls.suspect_flag, cls.offset
    
    @classmethod
    def not_eval(cls):
        return cls.not_eval_flag, cls.offset
    
    @classmethod
    def passes(cls):
        return cls.pass_flag, cls.offset

class NeighborTest:
    offset        = 31
    not_eval_flag = 0x1
    suspect_flag  = 0x2
    pass_flag     = 0x3
    
    @classmethod
    def suspect(cls):
        return cls.suspect_flag, cls.offset
    
    @classmethod
    def not_eval(cls):
        return cls.not_eval_flag, cls.offset
    
    @classmethod
    def passes(cls):
        return cls.pass_flag, cls.offset

class AggregateTest:
    offset        = 60
    fail_flag     = 0x0
    not_eval_flag = 0x1
    suspect_flag  = 0x2
    pass_flag     = 0x3
    
    @classmethod
    def fail(cls):
        return cls.fail_flag, cls.offset

    @classmethod
    def suspect(cls):
        return cls.suspect_flag, cls.offset
    
    @classmethod
    def not_eval(cls):
        return cls.not_eval_flag, cls.offset
    
    @classmethod
    def passes(cls):
        return cls.pass_flag, cls.offset



AvailableTests = [
    TimingGap,
    SyntaxCheck,
    GrossRangeTest,
    ClimatologyTest,
    SpikeTest,
    RateOfChangeTest,
    FlatLineTest,
    MultivariateTest,
    NeighborTest,
    AggregateTest
]


