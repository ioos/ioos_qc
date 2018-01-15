Usage
=====

There are two main ways to use the `ioos_qc` project.

- Manually calling test methods
- Using `QcConfig` objects


Manual
------



QcConfig
--------

A `QcConfig` object can be created and used to run checks against multiple streams of input data. To create a `QcConfig` object you may use a `dict` or a file system path in the form of a `str` or `Path` object.

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

.. code-block:: yaml
    :caption: Use a YAML file to load the `QcConfig`
    :linenos:

    qartod:
        gross_range_test:
            suspect_span: [1, 11]
            fail_span:
                - 0
                - 12

.. code-block:: python
    :linenos:
    :caption: With `str` path

    from ioos_qc.config import QcConfig

    with open(path) as f:
        q = QcConfig(f.read())
        
.. code-block:: python
    :linenos:
    :caption: With `Path` object

    from pathlib import Path
    from ioos_qc.config import QcConfig

    p = Path([path])
    with p.open() as f:
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
