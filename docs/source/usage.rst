Usage
=====

There are three main ways to use the `ioos_qc` project.

- Manual_
- QcConfig_
- NcQcConfig_


Manual
------



QcConfig
--------

A `QcConfig` object can be created and used to run checks against multiple streams of input data. To create a `QcConfig` object you may use a `dict` or a file system path in the form of a `str` or `Path` object.


You can create an `QcConfig` object from the following:

- python `dict` or `OrderedDict`
- JSON/YAML filepath (`str` or `Path` object)

Creation
~~~~~~~~

.. code-block:: python
    :linenos:
    :caption: Use a dict to load the QcConfig

    from ioos_qc.config import QcConfig

    config = {
        'qartod': {
            'gross_range_test': {
                'suspect_span': [1, 11],
                'fail_span': [0, 12],
            }
        }
    }
    q = QcConfig(config)


.. code-block:: python
    :linenos:
    :caption: With `str` path

    """
    qartod:
        gross_range_test:
            suspect_span: [1, 11]
            fail_span:
                - 0
                - 12
    """
    from ioos_qc.config import QcConfig

    with open(path) as f:
        q = QcConfig(f.read())


Running
~~~~~~~

Once you have a valid `QcConfig` object, the `run` method will execute all test that are configured within the object against any number of input arguments. The input arguments you supply to the `run` method should match the input arguments of each test. Refer to the API documentation for which tests require which input arguments.

For example, if I want to run the `gross_range_test` and the `location_test` using a single `QcConfig` object I would setup my object like so:

.. code-block:: python
    :linenos:

    from ioos_qc.config import QcConfig

    config = {
        'qartod': {
            'gross_range_test': {
                'suspect_span': [1, 11],
                'fail_span': [0, 12],
            }
            'location_test': {
                'bbox': [-100, -40, 100, 40]
            }
        }
    }
    q = QcConfig(config)

* `gross_range_test` requires the parameter `inp`.
* `location_test` requires the parameters `lon` and `lat`.

.. code-block:: python
    :linenos:

    results = q.run(
        inp=list(range(13))  # To satisfy `gross_range_test`
        lat=[ -41,  -40, -39, 0, 39,  40,  41],  # To satisfy `location_test`
        lon=[-101, -100, -99, 0, 99, 100, 101],  # To satisfy `location_test`
     )

All arguments can also be specified in the config object:

.. code-block:: python
    :linenos:

    config = {
        'qartod': {
            'gross_range_test': {
                'suspect_span': [1, 11],
                'fail_span': [0, 12],
                'inp': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 12]
            }
            'location_test': {
                'bbox': [-100, -40, 100, 40],
                'lat':  [ -41,  -40, -39, 0, 39,  40,  41],
                'lon':  [-101, -100, -99, 0, 99, 100, 101],
            }
        }
    }
    q = QcConfig(config)
    results = q.run()


Results
~~~~~~~

The results of a `QcConfig.run(...)` is a python OrderedDict that has the same structure as the
config object. The leaves (test parameters) are replaced by the results of each test.

.. code-block:: python
    :linenos:
    :caption: Example results object

    from ioos_qc.config import NcQcConfig

    config = {
        'qartod': {
            'gross_range_test': {
                'suspect_span': [1, 11],
                'fail_span': [0, 12],
            }
            'location_test': {
                'bbox': [-100, -40, 100, 40]
            }
        }
    }

    qc = QcConfig(config)
    results = qc.run(...)

    print(results)
    'qartod': {
        'gross_range_test': gross_range_tests return value,
        'location_test': location_test return value
    }



NcQcConfig
----------

A `NcQcConfig` object is meant to mimic how QC information would be stored and represented in a
netCDF file. It is composed of a top level key that is the variable name that should be the subject
of the QC checks defined underneath it. The checks are defined as `QcConfig` compatible objects.
You can think of `NcQcConfig` objects as linking a series of `QcConfig` objects to a single set
of input data.

You can create an `NcQcConfig` object from the following:

- python `dict` or `OrderedDict`
- JSON/YAML filepath (`str` or `Path` object)
- `netCDF4` filepath (`str` or `Path` object)
- `netCDF4.Dataset` object


Creation
~~~~~~~~

.. code-block:: python
    :linenos:
    :caption: Load a python dict

    from ioos_qc.config import NcQcConfig

    config = {
        'data1': {
            'qartod': {
                'gross_range_test': {
                    'suspect_span': [1, 11],
                    'fail_span': [0, 12],
                }
            }
        }
    }
    q = NcQcConfig(config)


.. code-block:: python
    :linenos:
    :caption: Use a `netCDF4.Dataset` to load the NcQcConfig

    from ioos_qc.config import NcQcConfig

    config = {
        'suspect_span': [1, 11],
        'fail_span': [0, 12]
    }
    data = range(10)

    with nc4.Dataset(path, 'w') as ncd:
        ncd.createDimension('time', len(data))
        data1 = ncd.createVariable('data1', 'f8', ('time',))
        data1.standard_name = 'air_temperature'
        data1[:] = data

        qc1 = ncd.createVariable('qc1', 'b')
        qc1.setncattr('ioos_qc_module', 'qartod')
        qc1.setncattr('ioos_qc_test',   'gross_range_test')
        qc1.setncattr('ioos_qc_target', 'data1')
        qc1.setncattr('ioos_qc_config', json.dumps(config))

    q = NcQcConfig(path)



Running
~~~~~~~

Once you have a valid `NcQcConfig` object, the `run` method will execute all of the tests that are configured within the object against a `netCDF.Dataset` or `netCDF4` file path. Like a `QcConfig` object, you may pass additional input parameters into the `run` method to passthough into the individual qc tests. `NcQcConfig` will automatically use the full array of values on each data variable you have defined (top level key in the config object) as the `inp` parameter. Refer to the API documentation for which tests require which input arguments.

The `run` method does not alter the passed in `netCDF4.Dataset` object (it is opened read only).
It is only used to pull the data from each variable to pass into each test.

For example, if I want to run the `gross_range_test` and the `location_test` using a single `QcConfig` object I would setup my object like so:

.. code-block:: python
    :linenos:

    from ioos_qc.config import NcQcConfig

    config = {
        'data1': {
            'qartod': {
                'gross_range_test': {
                    'suspect_span': [1, 11],
                    'fail_span': [0, 12]
                }
            }
        }
    }

    qc = NcQcConfig(config)

    # Note that the data from the netCDF variable `data1`
    # will be passed as the `inp` parameter automatically so
    # we can omit it here. We can also pass in `inp` manually
    # to override the default behavior if we so choose.
    ncresults = qc.run(path)


Results
~~~~~~~

The results of a `NcQcConfig.run(...)` is a python OrderedDict that has the same structure as the
config object. The leaves (test parameters) are replaced by the results of each test.

.. code-block:: python
    :linenos:
    :caption: Example results object

    from ioos_qc.config import NcQcConfig

    config = {
        'data1': {
            'qartod': {
                'gross_range_test': {
                    'suspect_span': [1, 11],
                    'fail_span': [0, 12]
                }
            }
        }
    }

    qc = NcQcConfig(config)
    results = qc.run(...)

    print(results)
    'data1': {
        'qartod': {
            'gross_range_test': gross_range_test return value
        }
    }


You can save a result object back to a netCDF file by calling the `save_to_netcdf` function on
an `NcQcConfig` object. This will alter the `netCDF4` file or `netCDF4.Dataset` object passed into
it by creating new variables for any tests present in the result object that are also present in the
config object. If existing variables matching the QC test are found it will update them with the
new result data and config attributes.

After saving a results object to a netCDF files, that file will be able to load the exact `NcQcConfig`
object used to define and run the quality variables available in the file. This is very powerful!


.. code-block:: python
    :linenos:
    :caption: Example save to netcdf and load config object again

    from ioos_qc.config import NcQcConfig

    config = {
        'data1': {
            'qartod': {
                'gross_range_test': {
                    'suspect_span': [1, 11],
                    'fail_span': [0, 12]
                }
            }
        }
    }

    qc1 = NcQcConfig(config)
    results1 = qc1.run(...)
    qc1.save_to_netcdf(path, results1)

    qc2 = NcQcConfig(path):
    results2 = qc2.run(...)

    assert results1 == results2
    assert qc1 == qc2
