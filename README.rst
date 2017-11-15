IOOS QC
=======

.. image:: https://travis-ci.org/ioos/qartod.svg?branch=master
   :target: https://travis-ci.org/ioos/qartod
   :alt: build_status

Collection of utilities, scripts and tests to assist in automated
quality assurance and quality control for oceanographic datasets and
observing systems.

Please see our project documentation `QARTOD Documentation <https://ioos.github.io/qartod/>`_

Available Tests
---------------

Current tests refer to the `QARTOD manuals <https://ioos.noaa.gov/project/qartod/>`_
Current tests are taken from the Wind, Water Level, Currents, In-Situ Temperature and Salinity QARTOD Manuals

Currently implemented tests are:

- Gross Range Test
- Attenuated Signal Test
- Flat Line Test
- Rate of Change Test

Running tests
-------------

To run tests, import the QARTOD qc module::

    from ioos_qc import qartod

Refer to the Sphinx generated documentation for information on how to use the
various QARTOD tests.

Caveats/Known Limitations
-------------------------

Currently, most methods in the QARTOD assume monotonically increasing,
time series data with evenly spaced intervals.  If there is an irregular or
large gap in time between successive data points, the tests as written will not
take this into account.
