#!/usr/bin/env python
'''
ioos_qartod.flags.waves

Flag Definitions for Waves
'''

class LTBulkWaveTest:
    offset        = 62
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

class LTStuckSensorTest:
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

class LTFrequencyRangeTest:
    offset        = 58
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

class LTLowFrequencyEnergyTest:
    offset        = 56
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

class LTRateOfChangeTest:
    offset        = 54
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

class SpikeTest:
    offset        = 20
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

class AcousticNoiseTest:
    offset        = 52
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

class SignalToNoiseTest:
    offset        = 50
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

class CorrelationMagnitudeTest:
    offset        = 48
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

class PressureAcousticSurfaceTrackerTest:
    offset        = 46
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

class AcousticCurrentVelocityRangeTest:
    offset        = 44
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

class AcousticCurrentVelocityMeanValueTest:
    offset        = 42
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

class SampleCountTest:
    offset        = 40
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

class STTimeSeriesGapTest:
    offset        = 38
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

class STTimeSeriesRangeTest:
    offset        = 36
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

class STTimeSeriesAccelerationTest:
    offset        = 34
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

class LTTimeSeriesCheckRatioCheckFactorTest:
    offset        = 32
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

class LTTimeSeriesMeanStandardDeviationTest:
    offset        = 8
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

class NeighborTest:
    offset        = 10
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
    offset        = 0
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
    LTBulkWaveTest,
    LTStuckSensorTest,
    LTFrequencyRangeTest,
    LTLowFrequencyEnergyTest,
    LTRateOfChangeTest,
    SpikeTest,
    AcousticNoiseTest,
    SignalToNoiseTest,
    PressureAcousticSurfaceTrackerTest,
    AcousticCurrentVelocityRangeTest,
    AcousticCurrentVelocityMeanValueTest,
    SampleCountTest,
    STTimeSeriesGapTest,
    STTimeSeriesRangeTest,
    STTimeSeriesAccelerationTest,
    LTTimeSeriesCheckRatioCheckFactorTest,
    LTTimeSeriesMeanStandardDeviationTest,
    NeighborTest
]

