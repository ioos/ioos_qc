Recurring climatology configurations
====================================

This offline example runs a QARTOD climatology test on synthetic sea
temperatures from two years. It uses the current ``Config`` and ``NumpyStream``
APIs; it does not need a netCDF file or a remote data service.

The configuration has three nested levels: the package (``qartod``), the test
(``climatology_test``), and its arguments. The test's ``config`` argument is a
list of climatology windows. Here, ``period: month`` makes ``tspan`` refer to
calendar months rather than absolute dates, so the same windows apply in both
years. January through March use an expected range of 10 to 13 degrees Celsius;
April through June use 11 to 14 degrees Celsius. These ranges are illustrative,
not recommended thresholds for a real station.

``zspan`` limits each window to depths from 0 to 100 metres. The example supplies
one depth per measurement, all zero. Time, depth and measurement arrays must
refer to the same observations. Use numeric depths, not an array with a datetime
dtype.

.. literalinclude:: examples/climatology_config.py
   :language: python
   :start-at: import numpy

``stream.run(config)`` yields results. ``collect_results(..., how="list")``
collects them into result objects; this example configures one test on one stream,
so ``collected[0].results`` contains its flags. A configuration with several tests
can produce several collected objects; inspect their ``stream_id``, ``package``
and ``test`` attributes rather than assuming the first is the test you want.

The printed flags repeat for the second year:

* ``1`` (GOOD): the value is inside that month's expected range.
* ``3`` (SUSPECT): the value is outside ``vspan``. No ``fspan`` is configured here,
  so these values are not assigned a FAIL flag.
* ``9`` (MISSING): the input measurement is NaN.

This example covers January through June at known, in-range depths. Add windows
for the other months when configuring a full-year dataset. The test does not
infer station-specific climatology or fill missing measurements. See
:func:`ioos_qc.qartod.climatology_test` for the complete test parameters and
:doc:`usage` for other stream and configuration formats.
