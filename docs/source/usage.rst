Usage
=====

At its core, ``ioos_qc`` is a collection of modules and methods to run various quality control checks on an input stream of data.

The following implementations are available in ``ioos_qc``:

* `IOOS QARTOD <https://ioos.noaa.gov/project/qartod/>`_ - `API </api/ioos_qc.html#module-ioos_qc.qartod>`_
* ARGO - `API </api/ioos_qc.html#module-ioos_qc.argo>`_

Basic usage
-----------

.. code-block:: python
    :linenos:
    :caption: Calling a test manually

    from ioos_qc import qartod

    results = qartod.gross_range_test(
        inp=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
        suspect_span=[0, 8],
        fail_span=[0, 10]
    )

    print(results)

    # prints a masked array with values:
    # [1, 1, 1, 1, 1, 1, 1, 1, 1, 3, 3, 4, 4]


In this example, we call the ``gross_range_test`` on a list of dummy data.
We've configured the test to fail if the data is outside the range ``[0, 10]`` and be marked suspect if outside ``[0, 8]``.
The test returns an array of qc results for each data point, where ``1`` is PASS, ``3`` is SUSPECT, and ``4`` is FAIL.


Motivation
----------

If all you want to do is run a one-time test against a stream of data, then all you really need is the example above.
However, in most projects, the hard part is not implementing the qc test methods themselves, rather it is problems such as:

* How to store QC test configurations and manage them?
* How to manage the inputs (data) going into the test, and the output (results) coming out?
* How to share QC result with your users in a consistent way that follows community standards?
* How to ensure that your test implementations perform well against large datasets?
* How to generate baseline QC configurations for a dataset?
* How to visualize and communicate QC results in a standard way?

The ``ioos_qc`` project does not just implement QC algorithms -- it attempts to help you with these problems as well.

The following sections explore concepts that ``ioos_qc`` uses to help you manage and run your tests efficiently.

Concepts
--------

There are three main concepts in the ``ioos_qc`` project:

- Configurations_: Standardized quality control definitions
- Streams_: Flexible data source classes to support running qualith checks against various data formats
- Stores_: Flexible data storage classes to support storing quality results in various data formats
- QcConfigCreator_: Classes to generate configuration objects based on external climatology datasets



Configurations
--------------

Configuration objects represent a collection of quality control tests to run and the parameters for each one.There three main types of `Config` objects:

- StreamConfig_: This configures QC tests for a single stream of data like a ``list``, ``tuple``, ``numpy.ndarray``, ``dask.array``, ``pandas.Series``, ``netCDF4.Variable``, or ``xarray.DataArray``. This can be used standalone, or as a building block for the following more complex configs.
- ContextConfig_: This defines a collection of ``StreamConfig`` objects. These can be applied to multiple variables provided in a ``pandas.DataFrame``, ``dask.DataFrame``, ``netCDF4.Dataset``, or ``xarray.Dataset``. Optionally, these configs can be constrained to specific time domains (``windows``) -- and/or spatial domains (``regions``).
- Config_: A collection of ``ContextConfig`` objects, suitable for configuring a single input dataset to be broken up by region and time window before having QC checks applied.

Each configuration type can be initialized with the following:

- python ``dict`` or ``OrderedDict``
- JSON/YAML filepath (``str`` or ``Path`` object), ``str``, or ``StringIO``

In addition, the ``ContextConfig`` and ``Config`` objects can be initialized with:

- netCDF4/xarray filepath (``str`` or ``Path`` object) or ``Dataset``


StreamConfig
~~~~~~~~~~~~
A ``StreamConfig`` object defines a specific `ioos_qc` test module and test function along with the configuration parameters in which to run it with.

.. note::

    In earlier versions, ``StreamConfig`` was known as ``QcConfig``.

Usage
^^^^^

.. code-block:: python
    :linenos:
    :caption: A basic ``StreamConfig`` object

    from ioos_qc.config import StreamConfig

    config = {
        'qartod': {
            'aggregate': {},
            'gross_range_test': {
                'suspect_span': [1, 11],
                'fail_span': [0, 12],
            }
        }
    }
    c = StreamConfig(config)


ContextConfig
~~~~~~~~~~~~~
A ``ContextConfig`` object defines multiple ``StreamConfig`` objects as well as optional `region` and `window` objects.

region
^^^^^^
A `GeoJSON` representation of a geographical region. This is processed into a ``shapely.geometry.GeometryCollection`` internally for intersection calculations.

window
^^^^^^
An object defining a time window using ``starting`` and ``ending``. Internally this is defined as

.. code-block:: python

    window = namedtuple(
        'TimeWindow',
        ('starting', 'ending'),
        defaults=[None, None]
    )

Usage
^^^^^

.. code-block:: python
    :linenos:
    :caption: A basic ``ContextConfig`` object

    from ioos_qc.config import ContextConfig

    config = """
        region: null
        window:
            starting: 2020-01-01T00:00:00Z
            ending: 2020-04-01T00:00:00Z
        streams:
            variable1:
                qartod:
                    location_test:
                        bbox: [-80, 40, -70, 60]
            variable2:
                qartod:
                    gross_range_test:
                        suspect_span: [1, 11]
                        fail_span: [0, 12]
    """
    c = ContextConfig(config)
    c = Config(config)  # Also loadable as a Config


Config
~~~~~~
The highest level and most flexible configuration object is a ``Config``. It can describe quality control configurations for any number of regions, windows and streams.


Usage
^^^^^

.. code-block:: python
    :linenos:
    :caption: A basic ``Config`` object

    from ioos_qc.config import Config

    config = """
        contexts:
            -   region: null
                window:
                    starting: 2020-01-01T00:00:00Z
                    ending: 2020-04-01T00:00:00Z
                streams:
                    variable1:
                        qartod:
                            location_test:
                                bbox: [-80, 40, -70, 60]
                    variable2:
                        qartod:
                            gross_range_test:
                                suspect_span: [1, 11]
                                fail_span: [0, 12]
            -   region: null
                window:
                    starting: 2020-01-01T00:00:00Z
                    ending: 2020-04-01T00:00:00Z
                streams:
                    variable1:
                        qartod:
                            location_test:
                                bbox: [-80, 40, -70, 60]
                    variable2:
                        qartod:
                            gross_range_test:
                                suspect_span: [1, 11]
                                fail_span: [0, 12]
    """
    c = Config(config)


Streams
-------

Streams represent the data input types for running quality control tests. A user "runs" a stream of data through a collection of quality control tests defined by a Config_. A list of possible Streams can be found in the `Streams API </api/ioos_qc.html#module-ioos_qc.streams>`_.
All streams return a generator of QC results that contain contextual information that can be useful when using the results. You can iterate over the results generator directly or you can collect them into more familiar ``list`` or ``dict`` objects before usage. If you are
working in a streaming environment you will want to use generator result objects yourself. If you are running one-time or batch process quality checks you likely want to collect the results or use one of the Store classes provided by ``ioos_qc``.


Results
~~~~~~~

Each yielded result is either a `StreamConfigResult </api/ioos_qc.html#ioos_qc.results.StreamConfigResult>`_ or a `ContextResult </api/ioos_qc.html#ioos_qc.results.ContextResult>`_, depending on what type of Config_ object was used. Collected results are only ever of one type, a `CollectedResult </api/ioos_qc.html#ioos_qc.results.CollectedResult>`_, and only one ``CollectedResult`` will be returned after collecting results for unique combination of ``stream_id`` and defined module/test. The benefit of using a ``CollectedResult`` is that it will piece back together all of the different ContextConfig_ objects in a Config_ and return you one result per unique stream_id and module/test combination.

For example: If you had a Config_ object that contained (3) different ContextConfig_ objects (each defining a time window and test inputs) for a single variable/``stream_id``, running that ``Config`` through any ``Stream`` implementation would yield (3) different ``ContextResult`` objects. You could use them yourself to construct whatever results you wanted to manually, or you could collect those results back into a single ``CollectedResult`` object to only have to deal with one result.

Historically, test results were returned in a ``dict`` structure. While this is still supported it should be considered deprecated. You should be using the individually yielded result objects or a list of `CollectedResult </api/ioos_qc.html#ioos_qc.results.CollectedResult>`_ objects in any applications (including Stores_ implementations) going forward.

.. code-block:: python
    :linenos:
    :caption: Different way to use Stream results

    import numpy as np
    import pandas as pd
    from ioos_qc.config import Config
    from ioos_qc.streams import PandasStream
    from ioos_qc.results import collect_results

    config = """
        contexts:
            -   window:
                    starting: 2020-01-01T00:00:00Z
                    ending: 2020-02-01T00:00:00Z
                streams:
                    variable1:
                        qartod:
                            aggregate:
                            gross_range_test:
                                suspect_span: [3, 4]
                                fail_span: [2, 5]
                    variable2:
                        qartod:
                            aggregate:
                            gross_range_test:
                                suspect_span: [23, 24]
                                fail_span: [22, 25]
            -   window:
                    starting: 2020-02-01T00:00:00Z
                    ending: 2020-03-01T00:00:00Z
                streams:
                    variable1:
                        qartod:
                            aggregate:
                            gross_range_test:
                                suspect_span: [43, 44]
                                fail_span: [42, 45]
                    variable2:
                        qartod:
                            aggregate:
                            gross_range_test:
                                suspect_span: [23, 24]
                                fail_span: [22, 25]
    """
    c = Config(config)

    rows = 50
    data_inputs = {
        'time': pd.date_range(start='01/01/2020', periods=rows, freq='D'),
        'z': 2.0,
        'lat': 36.1,
        'lon': -76.5,
        'variable1': np.arange(0, rows),
        'variable2': np.arange(0, rows),
    }
    df = pd.DataFrame(data_inputs)

    # Setup the stream
    ps = PandasStream(df)

    # Pass the run method the config to use
    results = ps.run(c)

    # results is a generator of ContextResult objects
    print(results)
    # <generator object PandasStream.run at ...>

    # list_collected is a list of CollectedResult objects
    # for each stream_id and module/test combination
    list_collected = collect_results(results, how='list')
    print(list_collected)
    # [
    #   CollectedResult(stream_id='variable1', package='qartod', test='gross_range_test', ...),
    #   CollectedResult(stream_id='variable1', package='qartod', test='aggregate', ...),
    #   CollectedResult(stream_id='variable2', package='qartod', test='gross_range_test', ...),
    #   CollectedResult(stream_id='variable2', package='qartod', test='aggregate', ...),
    # ]


NumpyStream
~~~~~~~~~~~

.. code-block:: python
    :linenos:
    :caption: An example of a NumpyStream

    import numpy as np
    import pandas as pd
    from ioos_qc.config import Config
    from ioos_qc.streams import NumpyStream

    config = """
        window:
            starting: 2020-01-01T00:00:00Z
            ending: 2020-04-01T00:00:00Z
        streams:
            variable1:
                qartod:
                    aggregate:
                    gross_range_test:
                        suspect_span: [20, 30]
                        fail_span: [10, 40]
    """
    c = Config(config)

    rows = 50
    tinp = pd.date_range(start='01/01/2020', periods=rows, freq='D').values
    inp = np.arange(0, tinp.size)
    zinp = np.full_like(tinp, 2.0)
    lat = np.full_like(tinp, 36.1)
    lon = np.full_like(tinp, -76.5)

    # Setup the stream
    ns = NumpyStream(inp, tinp, zinp, lat, lon)
    # Pass the run method the config to use
    results = ns.run(c)


PandasStream
~~~~~~~~~~~~

A PandasStream pulls all required information to run the qc tests from a single DataFrame. If the axes column names are not in ``time``, ``z``, ``lat``, ``lon`` or ``geom``, you may provide them as key word arguments. See the API docs for more information.

.. code-block:: python
    :linenos:
    :caption: An example of a PandasStream

    import numpy as np
    import pandas as pd
    from ioos_qc.config import Config
    from ioos_qc.streams import PandasStream

    config = """
        contexts:
            -   window:
                    starting: 2020-01-01T00:00:00Z
                    ending: 2020-02-01T00:00:00Z
                streams:
                    variable1:
                        qartod:
                            aggregate:
                            gross_range_test:
                                suspect_span: [3, 4]
                                fail_span: [2, 5]
                    variable2:
                        qartod:
                            aggregate:
                            gross_range_test:
                                suspect_span: [23, 24]
                                fail_span: [22, 25]
            -   window:
                    starting: 2020-02-01T00:00:00Z
                    ending: 2020-03-01T00:00:00Z
                streams:
                    variable1:
                        qartod:
                            aggregate:
                            gross_range_test:
                                suspect_span: [43, 44]
                                fail_span: [42, 45]
                    variable2:
                        qartod:
                            aggregate:
                            gross_range_test:
                                suspect_span: [23, 24]
                                fail_span: [22, 25]
    """
    c = Config(config)

    rows = 50
    data_inputs = {
        'time': pd.date_range(start='01/01/2020', periods=rows, freq='D'),
        'z': 2.0,
        'lat': 36.1,
        'lon': -76.5,
        'variable1': np.arange(0, rows),
        'variable2': np.arange(0, rows),
    }
    df = pd.DataFrame(data_inputs)

    # Setup the stream
    ps = PandasStream(df)
    # ps = PandasStream(df, time='time', z='z', lat='lat', lon='lon', geom='geom')
    # Pass the run method the config to use
    results = ps.run(c)


NetcdfStream
~~~~~~~~~~~~

A subset of the NumpyStream, the NetcdfStream simply extracts numpy arrays from variables within a netCDF file and passes them through as arrays to NumpyStream. If you are using this class you should look towards the XarrayStream class which subsets more efficiently.

.. code-block:: python
    :linenos:
    :caption: An example of a NetcdfStream

    import numpy as np
    import xarray as xr
    import pandas as pd
    from ioos_qc.config import Config
    from ioos_qc.streams import NetcdfStream

    config = """
        window:
            starting: 2020-01-01T00:00:00Z
            ending: 2020-04-01T00:00:00Z
        streams:
            variable1:
                qartod:
                    aggregate:
                    gross_range_test:
                        suspect_span: [20, 30]
                        fail_span: [10, 40]
    """
    c = Config(config)

    rows = 50
    data_inputs = {
        'time': pd.date_range(start='01/01/2020', periods=rows, freq='D'),
        'z': 2.0,
        'lat': 36.1,
        'lon': -76.5,
        'variable1': np.arange(0, rows),
    }
    df = pd.DataFrame(data_inputs)
    ds = xr.Dataset.from_dataframe(df)

    # Setup the stream
    ns = NetcdfStream(ds)
    # ns = NetcdfStream(ds, time='time', z='z', lat='lat', lon='lon')
    # Pass the run method the config to use
    results = ns.run(c)


XarrayStream
~~~~~~~~~~~~

.. code-block:: python
    :linenos:
    :caption: An example of a XarrayStream

    import numpy as np
    import xarray as xr
    import pandas as pd
    from ioos_qc.config import Config
    from ioos_qc.streams import XarrayStream

    config = """
        window:
            starting: 2020-01-01T00:00:00Z
            ending: 2020-04-01T00:00:00Z
        streams:
            variable1:
                qartod:
                    aggregate:
                    gross_range_test:
                        suspect_span: [20, 30]
                        fail_span: [10, 40]
    """
    c = Config(config)

    rows = 50
    data_inputs = {
        'time': pd.date_range(start='01/01/2020', periods=rows, freq='D'),
        'z': 2.0,
        'lat': 36.1,
        'lon': -76.5,
        'variable1': np.arange(0, rows),
    }
    df = pd.DataFrame(data_inputs)
    ds = xr.Dataset.from_dataframe(df)

    # Setup the stream
    xs = XarrayStream(ds)
    # xs = XarrayStream(ds, time='time', z='z', lat='lat', lon='lon')
    # Pass the run method the config to use
    results = xs.run(c)

Stores
------

Stores represent different data formats for storing quality control Results_ from Streams_. The results from any ``Stream`` should be able to be passed into any ``Store`` implementation defined in the `Stores API </api/ioos_qc.html#module-ioos_qc.stores>`_.


PandasStore
~~~~~~~~~~~

Collects all results and stores them as columns in a DataFrame.


CFNetCDFStore
~~~~~~~~~~~~~

Store the QC results in a CF compliant DSG type netCDF file, along with all metadata information and serializing the configuation used in the tests into the netCDF file. This currently only supports creating a new file with all results and does not support appending to existing files or results, although that is expected to be implemented at some pooint. You can also choose to store a subset of results in a file to support storing the aggregate results in one file and the individual test results in another file.


QcConfigCreator
---------------

A `QcConfigCreator` instance generates a config for `QcConfig` informed by reference datasets,
such as climatologies, defined via configuration.

CreatorConfig
~~~~~~~~~~~~~

CreatorConfig performs checks on the configuration to ensure that all required fields
and attributes are provided.

For convenience, the `get_assets.py` script is provided to download
and prepare climatology dataset from NARR and Ocean Atlas.


.. code-block:: python
    :linenos:
    :caption: Specify datasets and variables to be used by QcConfigCreator

    creator_config = {
        "datasets": [
            {
                "name": "ocean_atlas",
                "file_path": "assets/ocean_atlas.nc",
                "variables": {
                    "o2": "o_an",
                    "salinity": "s_an",
                    "temperature": "t_an"
                },
                "3d": "depth"
            },
            {
                "name": "narr",
                "file_path": "assets/narr.nc",
                "variables": {
                    "air": "air",
                    "pres": "slp",
                    "rhum": "rhum",
                    "uwnd": "uwnd",
                    "vwnd": "vwnd"
                }
            }
        ]
    }
    cc = CreatorConfig(creator_config)

    print(cc)
    {
        "narr": {
            "file_path": "assets/narr.nc",
            "variables": {
                "air": "air",
                "pres": "slp",
                "rhum": "rhum",
                "uwnd": "uwnd",
                "vwnd": "vwnd"
            }
        },
        "ocean_atlas": {
            "3d": "depth",
            "file_path": "assets/ocean_atlas.nc",
            "variables": {
                "o2": "o_an",
                "salinity": "s_an",
                "temperature": "t_an"
            }
        }
    }


QcConfigCreator
~~~~~~~~~~~~~~~
.. code-block:: python
    :linenos:
    :caption: Create QcConfigCreator using configuration just created

    qccc = QcConfigCreator(cc)

    print(qccc)
    {
        "narr": {
            "file_path": "assets/narr.nc",
            "variables": {
                "air": "air",
                "pres": "slp",
                "rhum": "rhum",
                "uwnd": "uwnd",
                "vwnd": "vwnd"
            }
        },
        "ocean_atlas": {
            "3d": "depth",
            "file_path": "assets/ocean_atlas.nc",
            "variables": {
                "o2": "o_an",
                "salinity": "s_an",
                "temperature": "t_an"
            }
        }
    }


QcVariableConfig
~~~~~~~~~~~~~~~~

An instance of *QcVariableConfig* specifies how quality control will be tested for a given variable.

In this example, the variable *air*, or air temperature, will be quality controlled based on climatological
data in the region defined by *bbox* (xmin, ymin, xmax, ymax), for a time range (between 2020-01-01 and 2020-01-08).
The *tests* sections specifies that two tests will be performed: *spike_test* and *gross_range_test*. Each
test section requires *suspect_min*, *suspect_max*, *fail_min*, and *fail_max* to be defined.

The *{fail,suspect}_{min,max}* values will be evaluated as functions with values for *min*, *max*, *mean*, and
*std* derived from the dataset for the bounds specified.  Note that each term, operator, and grouping symbol
must be surrounded by whitespace.

Test function allowed symbols:

- Data derived descriptive statistics: min, max, mean, std
- Operators: -, +, *, /
- Grouping symbols: (, )

Like CreatorConfig, QcVaribleConfig performs checks on the configuration to ensure that it adheres
to the specified schema and includes all required fields and attributes.

.. code-block:: python
    :linenos:

    qc_variable_config = {
        "variable": "air",
        "bbox": [-165, 70, 160, 80],
        "start_time": "2020-01-01",
        "end_time": "2020-01-08",
        "tests": {
            "spike_test": {
                "suspect_min": "1",
                "suspect_max": "( 1 + 2 )",
                "fail_min": "3 * 2 - 6",
                "fail_max": "3 * mean + std / ( max * min )"
            },
            "gross_range_test": {
                "suspect_min": "min - std * 2",
                "suspect_max": "max + std / 2",
                "fail_min": "mean * std",
                "fail_max": "mean / std"
            }
        }
    }
    vc = QcVariableConfig(qc_variable_config)
    print(vc)
    {
        "bbox": [
            -165,
            70,
            160,
            80
        ],
        "end_time": "2020-01-08",
        "start_time": "2020-01-01",
        "tests": {
            "gross_range_test": {
                "fail_max": "mean / std",
                "fail_min": "mean * std",
                "suspect_max": "max + std / 2",
                "suspect_min": "min - std * 2"
            },
            "spike_test": {
                "fail_max": "3 * mean + std / ( max * min )",
                "fail_min": "3 * 2 - 6",
                "suspect_max": "( 1 + 2 )",
                "suspect_min": "1"
            }
        }
    }


Create config for QcConfig
~~~~~~~~~~~~~~~~~~~~~~~~~~

Finally, the `QcConfigCreator` instance (`qccc`) takes the `QcVariableConfig` instance (`vc`)
and returns a config that can then be used with `QcConfig`.

.. code-block:: python
    :linenos:

    config = qccc.create_config(vc)
    print(json.dumps(config, indent=4, sort_keys=True))
    {
        "qartod": {
            "gross_range_test": {
                "fail_span": [
                    -224.23900165924232,
                    -2.673170364457356
                ],
                "suspect_span": [
                    -54.89132748864793,
                    7.09364403443822
                ]
            },
            "spike_test": {
                "fail_span": [
                    0.0,
                    -73.54932418742399
                ],
                "suspect_span": [
                    1.0,
                    3.0
                ]
            }
        }
    }
