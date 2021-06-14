#!python
"""Download and process source data used for ConfigCreator"""
import logging
import shutil
from pathlib import Path
from urllib import request

import netCDF4 as nc
import xarray as xr
from nco import Nco

logger = logging.getLogger('ConfigCreator:get_assets.py')
logger.setLevel(logging.INFO)

SOURCES = {
    'OCEAN_ATLAS': {
        'ts_url': 'https://data.nodc.noaa.gov/thredds/fileServer/ncei/woa/{0}/A5B7/1.00/woa18_A5B7_{1}{2:02d}_01.nc',
        'other_url': 'https://data.nodc.noaa.gov/thredds/fileServer/ncei/woa/{0}/all/1.00/woa18_all_{1}{2:02d}_01.nc'
    },
    'NARR': {
        'url': 'ftp://ftp.cdc.noaa.gov/Datasets/ncep.reanalysis.derived/surface/{0}'
    }
}


def ocean_atlas_download(output_dir, month, sources=SOURCES):
    variable_map = {
        'temperature': 't',
        'salinity': 's',
        'oxygen': 'o'
    }
    for name, name_in_file in variable_map.items():
        if name in ['temperature', 'salinity']:
            url = sources['OCEAN_ATLAS']['ts_url'].format(name, name_in_file, month.month)
        else:
            url = sources['OCEAN_ATLAS']['other_url'].format(name, name_in_file, month.month)
        r = request.urlopen(url)
        data = r.read()

        fname = f'ocean_atlas_{name}_{month.month:02}.nc'
        out_file = output_dir / fname
        with open(out_file, 'wb') as f:
            f.write(data)


def ocean_atlas_merge_variables(output_dir, month):
    ocean_atlas_files = output_dir.glob(f'ocean_atlas_*_{month.month:02}.nc')
    ocean_atlas_files = list(ocean_atlas_files)
    ocean_atlas_files.sort()
    outfile = output_dir / f'ocean_atlas_{month.month:02}.nc'

    nco = Nco()
    options = [
        '-A',
        '-4',
        '-L 5'
    ]
    outfile = shutil.copy(ocean_atlas_files[0], outfile)
    for f in ocean_atlas_files[1::]:
        nco.ncks(input=str(f), output=str(outfile), options=options)


def ocean_atlas_variable_enhance(output_dir, month):
    fname = output_dir / f'ocean_atlas_{month.month:02}.nc'

    # only keep variables needed
    nco = Nco()
    vars_to_keep = ','.join(['s_an', 't_an', 'o_an'])
    options = [
        '-h',
        f'-v {vars_to_keep}'
    ]
    nco.ncks(input=str(fname), output=str(fname), options=options)

    # original time in months from 1900-01-01.
    # - force to days since 1970-01-01 for first of each month
    # - makes it easy to use with xarray
    new_time_units = 'days since 1970-01-01'
    num = nc.date2num(month, new_time_units)
    with nc.Dataset(fname, 'a') as ncds:
        tvar = ncds.variables['time']
        tvar[:] = num
        tvar.units = new_time_units
        tvar.calendar = 'standard'

    # make time a record
    options = [
        '-O',
        '--mk_rec_dmn time'
    ]
    nco.ncks(input=str(fname), output=str(fname), options=options)


def ocean_atlas_merge_time(output_dir):
    variable_merged_files = output_dir.glob('ocean_atlas_??.nc')
    variable_merged_files = [str(merged_file) for merged_file in list(variable_merged_files)]
    variable_merged_files.sort()
    output_file = output_dir.parent / 'ocean_atlas.nc'

    nco = Nco()
    options = [
        '-A'
    ]
    nco.ncrcat(input=variable_merged_files, output=str(output_file), options=options)


def ocean_atlas_enhance(output_dir):
    output_file = output_dir.parent / 'ocean_atlas.nc'
    output_tmp_file = output_dir.parent / 'ocean_atlas_tmp.nc'

    # change value that fits in a packed short var
    nco = Nco()
    options = [
        '-O',
        '-a _FillValue,,o,f,-127'
    ]
    nco.ncatted(input=str(output_file), output=str(output_tmp_file), options=options)

    # pack to use bytes
    # - requires output file defined with -o option
    options = [
        '-O',
        '-M flt_byt',
        f"-o {str(output_file)}"
    ]
    nco.ncpdq(input=str(output_tmp_file), output=str(output_file), options=options)


def get_ocean_atlas(output_dir):
    time_range = xr.cftime_range(start='2018', end='2018-12-31', freq='MS')
    for month in time_range:
        logger.info(f'downloading Ocean Atlas for {month}')
        ocean_atlas_download(output_dir, month)
        ocean_atlas_merge_variables(output_dir, month)
        ocean_atlas_variable_enhance(output_dir, month)
    ocean_atlas_merge_time(output_dir)
    ocean_atlas_enhance(output_dir)


def narr_download(output_dir, sources=SOURCES):
    variables = {
        'air': 'air.sig995.mon.ltm.nc',
        'rhum': 'rhum.sig995.mon.ltm.nc',
        'slp': 'slp.mon.ltm.nc',
        'uwind': 'uwnd.sig995.mon.ltm.nc',
        'vwind': 'vwnd.sig995.mon.ltm.nc'
    }
    for variable_name, variable_file in variables.items():
        url = sources['NARR']['url'].format(variable_file)
        r = request.urlopen(url)
        data = r.read()

        fname = f'narr_{variable_name}.nc'
        out_file = output_dir / fname
        with open(out_file, 'wb') as f:
            f.write(data)


def narr_merge_variables(output_dir):
    narr_files = output_dir.glob('narr_*.nc')
    narr_files = list(narr_files)
    narr_files.sort()
    outfile = output_dir.parent / 'narr.nc'

    nco = Nco()
    options = [
        '-A',
        '-4',
        '-L 5'
    ]
    outfile = shutil.copy(narr_files[0], outfile)
    for f in narr_files[1::]:
        nco.ncks(input=str(f), output=str(outfile), options=options)


def narr_enhance(output_dir):
    outfile = output_dir.parent / 'narr.nc'
    outtmp = output_dir.parent / 'narr_tmp.nc'

    # remove unnecessary vars
    nco = Nco()
    options = [
        '-O',
        '-C',
        '-x',
        '-v climatology_bounds,valid_yr_count'
    ]
    nco.ncks(input=str(outfile), output=str(outfile), options=options)

    # change lon from [0, 360) to [-180, 180)
    options = [
        '-O',
        '--msa',
        '-d lon,181.,360.',
        '-d lon,0.,180.0',
    ]
    nco.ncks(input=str(outfile), output=str(outtmp), options=options)

    options = [
        '-O',
        "-s 'where(lon > 180) lon=lon-360'"
    ]
    nco.ncap2(input=str(outtmp), output=str(outfile), options=options)

    # times don't work with xarray, so the year is changed to the middle of climatology
    # climatology: 1981-01-01 - 2010-12-31
    # midyear is 1996
    time_range = xr.cftime_range(start='1996', end='1996-12-31', freq='MS')
    new_units = 'days since 1970-01-01 00:00:00'
    new_times = nc.date2num(time_range, units=new_units)
    with nc.Dataset(outfile, 'a') as ncd:
        time = ncd.variables['time']
        time[:] = new_times
        time.units = new_units


def get_narr(output_dir):
    logger.info('downloading NARR')
    narr_download(output_dir)
    narr_merge_variables(output_dir)
    narr_enhance(output_dir)


def remove_tmp_files(dirs_to_delete):
    logger.info('removing tmp files')
    for dir in dirs_to_delete:
        logger.info(f'removing {dir}')
        shutil.rmtree(str(dir))


def main(output_dir, remove_tmp_files=False):
    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f'Downloading and saving data for QcConfigCreator to {output_dir}')
    logger.info('Downloading Ocean Atlas')
    ocean_atlas_dir = output_dir / 'ocean_atlas'
    ocean_atlas_dir.mkdir(exist_ok=True)
    get_ocean_atlas(ocean_atlas_dir)

    logger.info('Downloading NARR')
    narr_dir = output_dir / 'narr'
    narr_dir.mkdir(exist_ok=True)
    get_narr(narr_dir)

    if remove_tmp_files:
        remove_tmp_files([ocean_atlas_dir, narr_dir])


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser('Download source data for ConfigCreator')
    parser.add_argument(
        '-o',
        '--outdir',
        type=str,
        default='resources',
        help='Output path for downloaded data'
    )
    parser.add_argument(
        '-d',
        '--delete',
        action='store_true',
        help='Remove temporary files'
    )
    args = parser.parse_args()
    main(args.outdir, args.delete)
