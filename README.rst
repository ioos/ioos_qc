=======
IOOS QC
=======

.. image:: https://travis-ci.org/axiom-data-science/ioos_qc.svg?branch=master
   :target: https://travis-ci.org/axiom-data-science/ioos_qc
   :alt: build_status

Collection of utilities, scripts and tests to assist in automated
quality assurance and quality control for oceanographic datasets and
observing systems.

Please see the `IOOS QC project documentation <https://axiom-data-science.github.io/ioos_qc/>`_ for additional information.

Available Tests
---------------

QARTOD
======

Current tests refer to the `QARTOD manuals <https://ioos.noaa.gov/project/qartod/>`_ and are taken from the Wind, Water Level, Currents, In-Situ Temperature and Salinity manuals

Currently implemented tests are:

- `Location Test <https://axiom-data-science.github.io/ioos_qc/api/ioos_qc.html#ioos_qc.qartod.location_test>`_
- `Gross Range Test <https://axiom-data-science.github.io/ioos_qc/api/ioos_qc.html#ioos_qc.qartod.gross_range_test>`_
- `Climatology Test <https://axiom-data-science.github.io/ioos_qc/api/ioos_qc.html#ioos_qc.qartod.climatology_test>`_
- `Spike Test <https://axiom-data-science.github.io/ioos_qc/api/ioos_qc.html#ioos_qc.qartod.spike_test>`_
- `Rate of Change Test <https://axiom-data-science.github.io/ioos_qc/api/ioos_qc.html#ioos_qc.qartod.rate_of_change_test>`_
- `Flat Line Test <https://axiom-data-science.github.io/ioos_qc/api/ioos_qc.html#ioos_qc.qartod.flat_line_test>`_
- `Attenuated Signal Test <https://axiom-data-science.github.io/ioos_qc/api/ioos_qc.html#ioos_qc.qartod.attenuated_signal_test>`_

Running tests
-------------

To run tests, import the correct module::

    from ioos_qc import qartod

You can then run any test you would like::

    qartod.location_test(...)
    qartod.spike_test(...)
    ...

Refer to the `documentation <https://axiom-data-science.github.io/ioos_qc/>`_ for information on how to use the various tests.

Caveats/Known Limitations
-------------------------

Currently, most methods in the QARTOD assume monotonically increasing,
time series data with evenly spaced intervals.  If there is an irregular or
large gap in time between successive data points, the tests as written will not
take this into account.
