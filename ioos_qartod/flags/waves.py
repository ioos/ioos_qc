#!/usr/bin/env python
'''
ioos_qartod.flags.waves

Flag Definitions for Waves
'''

class LTBulkWaveTest:
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

class LTFrequencyRangeTest:
    offset        = 3
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
    offset        = 7
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

class AcousticNoiseTest:
    offset        = 9
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
    offset        = 12
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

class LTRateOfChangeTest:
    offset        = 18
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

class LTStuckSensorTest:
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

class CorrelationMagnitudeTest:
    offset        = 24
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

class SignalStrengthTest:
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

class PressureAcousticSurfaceTrackerTest:
    offset        = 30
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

class TSCurveSpaceTest:
    offset        = 33
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

class AcousticCurrentVelocityRangeTest:
    offset        = 36
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
    offset        = 39
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

class STTimeSeriesGapTest:
    offset        = 45
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

class STTimeSeriesAccelerationTest:
    offset        = 51
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
    offset        = 54
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
    offset        = 57
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
    TSCurveSpaceTest,
    LTTimeSeriesMeanStandardDeviationTest
]

