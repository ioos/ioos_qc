import logging

import numpy as np
from bokeh import plotting
from bokeh.layouts import gridplot

from ioos_qc.config import Config
from ioos_qc.streams import XarrayStream
from ioos_qc.stores import CFNetCDFStore
from ioos_qc.results import collect_results
from ioos_qc.config_creator import CreatorConfig, QcConfigCreator, QcVariableConfig
from ioos_qc.plotting import bokeh_plot_collected_result

from pocean.dsg import IncompleteMultidimensionalTrajectory


L = logging.getLogger(__name__)


def test_erddap_testing():
    erddap_server = 'https://ferret.pmel.noaa.gov/pmel/erddap'
    dataset_id = 'sd1035_2019'
    dataset_id = 'sd1041_2019'
    #dataset_id = 'sd1055'
    # dataset_id = 'saildrone_arctic_data'
    # dataset_id = 'fisheries_2020_all'
    dataset_id = 'sd1069'

    from erddapy import ERDDAP
    e = ERDDAP(
        server=erddap_server,
        protocol='tabledap',
    )
    e.response = 'csv'
    e.dataset_id = dataset_id

    ds = e.to_xarray()
    ds

    # Dataset level metadata to drive climatology extraction
    min_t = str(ds.time.min().dt.floor("D").dt.strftime("%Y-%m-%d").data)
    max_t = str(ds.time.max().dt.ceil("D").dt.strftime("%Y-%m-%d").data)
    min_x = float(ds.longitude.min().data)
    min_y = float(ds.latitude.min().data)
    max_x = float(ds.longitude.max().data)
    max_y = float(ds.latitude.max().data)
    bbox = [min_x, min_y, max_x, max_y]

    # Configure how each variable's config will be generated
    default_config = {
        "bbox": bbox,
        "start_time": min_t,
        "end_time": max_t,
        "tests": {
            "spike_test": {
                "suspect_threshold": "1",
                "fail_threshold": "2"
            },
            "gross_range_test": {
                "suspect_min": "min - std * 2",
                "suspect_max": "max + std / 2",
                "fail_min": "mean / std",
                "fail_max": "mean * std"
            }
        }
    }

    # For any variable name or standard_name you can define a custom config
    custom_config = {
        'air_temperature': {
            "variable": "air"
        },
        'air_pressure': {
            "variable": "pres"
        },
        'relative_humidity': {
            "variable": "rhum"
        },
        'sea_water_temperature': {
            "variable": "temperature"
        },
        'sea_water_practical_salinity': {
            "variable": "salinity"
        },
        'eastward_wind': {
            "variable": "uwnd"
        },
        'northward_wind': {
            "variable": "vwnd"
        }
    }

    # Generate climatology configs
    creator_config = {
        "datasets": [
            {
                "name": "ocean_atlas",
                "file_path": "resources/ocean_atlas.nc",
                "variables": {
                    "o2": "o_an",
                    "salinity": "s_an",
                    "temperature": "t_an"
                },
                "3d": "depth"
            },
            {
                "name": "narr",
                "file_path": "resources/narr.nc",
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
    qccc = QcConfigCreator(cc)

    # Break down variable by standard name
    def not_stddev(v):
        return v and not v.endswith(' SD')

    # air_temp_vars = ds.filter_by_attrs(long_name=not_stddev, standard_name='air_temperature')
    # pressure_vars = ds.filter_by_attrs(long_name=not_stddev, standard_name='air_pressure')
    # humidity_vars = ds.filter_by_attrs(long_name=not_stddev, standard_name='relative_humidity')
    # water_temp_vars = ds.filter_by_attrs(long_name=not_stddev, standard_name='sea_water_temperature')
    # salinity_vars = ds.filter_by_attrs(long_name=not_stddev, standard_name='sea_water_practical_salinity')
    # uwind_vars = ds.filter_by_attrs(long_name=not_stddev, standard_name='eastward_wind')
    # vwind_vars = ds.filter_by_attrs(long_name=not_stddev, standard_name='northward_wind')
    # all_vars = [air_temp_vars, pressure_vars, humidity_vars, water_temp_vars, salinity_vars, uwind_vars, vwind_vars]
    # all_vars

    air_temp = ['air_temperature']
    pressure = ['air_pressure']
    humidity = ['relative_humidity']
    water_temp = ['sea_water_temperature']
    salt = ['sea_water_practical_salinity']
    u = ['eastward_wind']
    v = ['northward_wind']

    run_tests = air_temp + pressure + humidity + water_temp + salt + u + v
    final_config = {}

    for v in ds:
        da = ds[v]

        # Don't run tests for unknown variables
        if 'standard_name' not in da.attrs or da.attrs['standard_name'] not in run_tests:
            continue

        # The standard names are identical for the mean and the stddev
        # so ignore the stddev version of the variable
        if v.endswith('_STDDEV'):
            continue

        config = default_config.copy()

        min_t = str(da.time.min().dt.floor("D").dt.strftime("%Y-%m-%d").data)
        max_t = str(da.time.max().dt.ceil("D").dt.strftime("%Y-%m-%d").data)
        min_x = float(da.longitude.min().data)
        min_y = float(da.latitude.min().data)
        max_x = float(da.longitude.max().data)
        max_y = float(da.latitude.max().data)
        bbox = [min_x, min_y, max_x, max_y]

        config["bbox"] = bbox
        config["start_time"] = min_t
        config["end_time"] = max_t

        # Allow custom overrides on a variable name basis
        if v in custom_config:
            config.update(custom_config[v])

        # Allow custom overrides on a standard_name name basis
        if da.attrs['standard_name'] in custom_config:
            config.update(custom_config[da.attrs['standard_name']])

        # Generate the ioos_qc Config object
        qc_var = QcVariableConfig(config)
        qc_config = qccc.create_config(qc_var)

        # Strip off the variable that create_config added
        qc_config = list(qc_config.values())[0]

        # Add it to the final config
        final_config[v] = qc_config

    c = Config(final_config)
    xs = XarrayStream(ds, time='time', lat='latitude', lon='longitude')
    qc_results = xs.run(c)

    # Plotting code
    # all_results = collect_results(qc_results, how='list')
    # # spike tests dont work with nan values so it causes issue
    # # with the shared time coordinate variable. Some variables
    # # only output every 5 readings
    # # https://ferret.pmel.noaa.gov/pmel/erddap/tabledap/sd1069.htmlTable?UWND_MEAN%2CVWND_MEAN%2CTEMP_AIR_MEAN%2Clatitude%2Clongitude%2Ctime&time%3E=2020-10-24&time%3C=2020-10-26T18%3A59%3A00Z
    # new_ds = ds.isel(dict(obs=slice(None, None, 5)))
    # new_xs = XarrayStream(new_ds, time='time', lat='latitude', lon='longitude')
    # new_qc_results = new_xs.run(c)
    # every_five_results = collect_results(new_qc_results, how='list')

    # plots = []
    # for i, lr in enumerate(all_results):
    #     if lr.data.any() and lr.results.any():
    #         if not np.isnan(lr.data[1:101:5]).all():
    #             print(f"plotting all for {lr.stream_id}")
    #             plot = bokeh_plot_collected_result(lr)
    #         else:
    #             print(f"plotting every 5 for {lr.stream_id}")
    #             plot = bokeh_plot_collected_result(every_five_results[i])
    #         plots.append(plot)

    # kwargs = {
    #     'merge_tools': True,
    #     'toolbar_location': 'above',
    #     'sizing_mode': 'scale_width',
    #     'plot_width': 600,
    #     'plot_height': 280,
    #     'ncols': 2
    # }
    # gp = gridplot(plots, **kwargs)
    # plotting.show(gp)

    # Save a netCDF file
    ncd = CFNetCDFStore(qc_results)
    ncd.save(
        'results.nc',
        IncompleteMultidimensionalTrajectory,
        c,
        dsg_kwargs=dict(
            reduce_dims=True,
            unlimited=False,
            unique_dims=True
        )
    )
