Usage
=====

Basic usage
-----------

At its core, ``ioos_qc`` is a set of methods to run quality control checks on an input stream of data.

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

The ``ioos_qc`` project does not just implement QC algorithms -- it attempts to help you with these problems as well.

The following sections explore concepts that ``ioos_qc`` uses to help you manage and run your tests efficiently.

Concepts
--------

There are three main concepts in the ``ioos_qc`` project:

- Configurations_: Quality control configuration
- Streams_: Data source to run quality checks against
- Stores_: Storage for the quality control results



Configurations
--------------

Configuration objects represent a collection of quality control tests to run and the parameters for each one.There three main types of `Config` objects:

- StreamConfig_: This configures QC tests for a single stream of data like a ``list``, ``tuple``, ``numpy.ndarray``, ``dask.array``, ``pandas.Series``, ``netCDF4.Variable``, or ``xarray.DataArray``. This can be used standalone, or as a building block for the following more complex configs.
- ContextConfig_: This defines a collection of ``StreamConfig`` objects. These can be applied to multiple variables provided in a ``pandas.DataFrame``, ``dask.DataFrame``, ``netCDF4.Dataset``, or ``xarray.Dataset``. Optionally, these configs can be constrained to specific time domains (``window``'s) -- and/or spatial domains (``region``'s).
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
An object defining a time window using ``starting`` and ``ending``. Interally this is defined as

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

Streams represent the data input types for running quality control tests. A user "runs" a stream of data through a collection of quality control tests defined by a Config_. A list of possible Streams can be found in the :ref:`Streams API<ioos\_qc.streams module>`.


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

**TODO there's nothing here??**