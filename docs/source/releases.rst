IOOS QC Releases and Migration Guide
====================================

1.0.0 (March 2020)
###################

As of this release, all of the existing tests in the QARTOD API should be stable.

Special thanks to Andrew Reed, Seth Foote, Elizabeth Dobbins, Jesse Lopez, and Charles Seaton for reviewing code,
improving documentation, and submitting issues and pull requests.

Changes from **0.2.1**:

* :code:`NcQcConfig` updates
    * Improve documentation and tests
    * Update :code:`save_to_netcdf` method to make it compliant with latest CF and IOOS metadata profiles
        * See `cf-conventions #216 <https://github.com/cf-convention/cf-conventions/issues/216>`_ and `ioos_qc #14 <https://github.com/ioos/ioos_qc/issues/14>`_)
    * Add option for :code:`NcQcConfig` to generate aggregate flag (see `#15 <https://github.com/ioos/ioos_qc/issues/15>`_)
* Attenuated signal test rewrite
    * See `#12 <https://github.com/ioos/ioos_qc/pull/12>`_ and `#29 <https://github.com/ioos/ioos_qc/pull/29>`_
* Climatology test updates
    * Use relative time for ranges (`#19 <https://github.com/ioos/ioos_qc/issues/19>`_)
    * `span bugfixes <https://github.com/ioos/ioos_qc/pull/28>`_
    * `periods bugfix <https://github.com/ioos/ioos_qc/pull/22>`_
* Fix :code:`datetime64[ns]` bugs (`#16 <https://github.com/ioos/ioos_qc/issues/16>`_)
* Added performance test suite
* Support for python 3.8
* Removed python 3.5 support
* Improved documentation and notebook examples

If you were previously using https://github.com/ioos/qartod , see the *Migrating* section below.

0.2.1 (Sept 9, 2019)
####################

* Bugfix: flat line test handles short :code:`inp`

0.2.0 (Sept 9, 2019)
####################

* Flat line test rewrite
    * Includes: a bug fix to handle a flat line starting at the beginning of the timeseries, massive speed improvements, and a slight change to the algorithm to find flat lines
    * See (`PR #11 <https://github.com/ioos/ioos_qc/pull/11>`_) for more information
* Add Jupyter notebook examples to the docs

0.1.1 (Feb 18, 2019)
####################

* Remove :code:`dask` as a requirement (`#5 <https://github.com/ioos/ioos_qc/pull/5>`_)

0.1.0 (Feb 8, 2019)
###################

* Complete test overhaul
    * Rewrite of tests to improve readability and maintainability
    * Introduce :code:`QcConfig` concept
    * Tests use time interval parameters instead of counts (`#2 <https://github.com/ioos/ioos_qc/pull/2>`_)
    * Split out :code:`threshold` parameters into explicit :code:`suspect` and :code:`fail` parameters (`#4 <https://github.com/ioos/ioos_qc/pull/4>`_)
    * Bug fixes
* Remove support for python 2
* Upgrade numpy to 1.14
* Build, test, and documentation improvements


Migrating from 0.0.3 to 0.1.0
****************************************

Both versions implement the same QARTOD tests using essentially the same algorithms, however the test implementations are much different.

We recommend going test by test and comparing the documentation: `0.0.3 <https://ioos.github.io/qartod/code/qc.html>`_ versus `latest <https://ioos.github.io/ioos_qc/api/ioos_qc.html#module-ioos_qc.qartod>`_.

Specific changes to be aware of include:

* Removing support for python 2 and requiring :code:`numpy>=1.14`.
* Module rename: :code:`ioos_qartod.qc_tests.qc.*` to :code:`ioos_qc.qartod.*`
* Test renames
    * :code:`attenuated_signal_check --> attenuated_signal_test`
    * :code:`climatology_check --> climatology_test`
    * :code:`flat_line_check --> flat_line_test`
    * :code:`location_set_check --> location_test`
    * :code:`range_check --> gross_range_test`
    * :code:`rate_of_change_check --> rate_of_change_test`
    * :code:`spike_check --> spike_test`
* Tests use time interval parameters instead of counts (See `#2 <https://github.com/ioos/ioos_qc/pull/2>`_)
    * This makes the test agnostic about sampling frequency, and thus more generic and human-readable
    * For example, you can specify a rate of change threshold of :code:`0.5 units/second` instead of :code:`0.5 units/count`
    * Test example: old `flat_line_check <https://ioos.github.io/qartod/code/qc.html#ioos_qartod.qc_tests.qc.flat_line_check>`_ versus new `flat_line_test <https://ioos.github.io/ioos_qc/api/ioos_qc.html#ioos_qc.qartod.flat_line_test>`_
* Text use explicit :code:`suspect_threshold` and :code:`fail_threshold` parameters instead of a single :code:`threshold` parameters (See `#3 <https://github.com/ioos/ioos_qc/pull/4>`_)
    * This improves readability, especially for users not familiar with the code
* Introduction of :code:`QcConfig` object
    * While you can still call test methods directly, we highly recommend using the :code:`QcConfig` object instead
    * This object encapsulates multiple test configurations, including test parameters, into a single object that can be serialized as JSON for extra portability
    * See the :doc:`Quickstart notebook example </examples/Qartod_Single_Test_Example>` and :doc:`QcConfig Usage page </usage>` for more info and examples

0.0.3 (Sept 8, 2016)
####################

See https://github.com/ioos/qartod and https://ioos.github.io/qartod/

