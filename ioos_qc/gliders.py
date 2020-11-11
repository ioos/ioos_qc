#!/usr/bin/env python
# coding=utf-8
"""Deprecated module. Consider using ARGO instead."""
import warnings

from ioos_qc import argo


def pressure_check(inp):
    warnings.warn("gliders.pressure_check has been replaced by argo.pressure_increasing_test", DeprecationWarning)
    return argo.pressure_increasing_test(inp)
