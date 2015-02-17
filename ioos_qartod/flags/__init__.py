#!/usr/bin/env python
'''
ioos_qartod.flags


'''
import numpy as np

class QARTODEval:
    '''
    Base class to define flag sets
    '''

    def __init__(self, flag=0x0):
        self.flag = flag

    def mark(self, test_flag, test_offset):
        a = test_flag << test_offset
        self.flag = a ^ ((a ^ self.flag) & ~(3 << test_offset))
        return self.flag

    def mark_test(self, test):
        return self.mark(*test)

    def check(self, test_obj):
        return (self.flag & (3 << test_obj.offset)) >> (test_obj.offset)

    def check_str(self, test_obj):
        flag = self.check(test_obj)
        if hasattr(flag, '__iter__'):
            return self._check_strs(flag, test_obj)
        return self._check_str(flag, test_obj)

    def _check_str(self, flag, test_obj):
        for k in [k for k in vars(test_obj) if 'flag' in k]:
            if getattr(test_obj, k) == flag:
                return k
        return None

    def _check_strs(self, flags, test_obj):
        return [self._check_str(flag, test_obj) for flag in flags]

