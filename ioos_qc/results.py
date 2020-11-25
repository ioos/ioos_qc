#!/usr/bin/env python
# coding=utf-8
from dataclasses import dataclass
from collections import OrderedDict as odict
from collections import namedtuple, defaultdict

import numpy as np
import pandas as pd
from ioos_qc.qartod import QartodFlags


StreamConfigResult = namedtuple(
    'StreamConfigResult', [
        'package',
        'test',
        'function',
        'results'
    ]
)

ContextResult = namedtuple(
    'ContextResult', [
        'stream_id',
        'results',
        'subset_indexes',
        'data',
        'tinp',
        'zinp',
        'lat',
        'lon'
    ]
)


@dataclass
class CollectedResult:
    stream_id: str
    package: str
    test: str
    function: callable
    results: np.ma.core.MaskedArray = None
    data: np.ndarray = None
    tinp: np.ndarray = None
    zinp: np.ndarray = None
    lat: np.ndarray = None
    lon: np.ndarray = None

    def function_name(self) -> str:
        return self.function.__name__

    @property
    def hash_key(self) -> str:
        return f'{self.stream_id}: {self.package}.{self.test}'


def collect_results(results, how='list'):
    if how in ['list', list]:
        return collect_results_list(results)
    elif how in ['dict', dict]:
        return collect_results_dict(results)


def collect_results_list(results):
    """ Turns a list of ContextResult objects into an iterator of CollectedResult objects
        by combining the subset_index information in each ContextResult together into
        a single array of results.
    """
    collected = odict()

    # ContextResults
    for r in results:

        # Shortcut for StreamConfigResult objects when someone uses QcConfig.run() directly
        # and doesn't go through a Stream object
        if isinstance(r, StreamConfigResult):
            cr = CollectedResult(
                stream_id=None,
                package=r.package,
                test=r.test,
                function=r.function,
                results=r.results,
            )
            collected[cr.hash_key] = cr
            continue

        # StreamConfigResults
        for tr in r.results:

            cr = CollectedResult(
                stream_id=r.stream_id,
                package=tr.package,
                test=tr.test,
                function=tr.function
            )

            if cr.hash_key not in collected:
                # Set the initial values
                cr.results = np.ma.masked_all(shape=r.subset_indexes.shape, dtype=r.data.dtype)
                cr.data = np.ma.masked_all(shape=r.subset_indexes.shape, dtype=r.data.dtype)
                cr.tinp = np.ma.masked_all(shape=r.subset_indexes.shape, dtype=r.tinp.dtype)
                cr.zinp = np.ma.masked_all(shape=r.subset_indexes.shape, dtype=r.zinp.dtype)
                cr.lat = np.ma.masked_all(shape=r.subset_indexes.shape, dtype=r.lat.dtype)
                cr.lon = np.ma.masked_all(shape=r.subset_indexes.shape, dtype=r.lon.dtype)
                collected[cr.hash_key] = cr

            collected[cr.hash_key].results[r.subset_indexes] = tr.results

        collected[cr.hash_key].data[r.subset_indexes] = r.data
        collected[cr.hash_key].tinp[r.subset_indexes] = r.tinp
        collected[cr.hash_key].zinp[r.subset_indexes] = r.zinp
        collected[cr.hash_key].lat[r.subset_indexes] = r.lat
        collected[cr.hash_key].lon[r.subset_indexes] = r.lon

    return list(collected.values())


def collect_results_dict(results):
    """ Turns a list of ContextResult objects into a dictionary of test results
        by combining the subset_index information in each ContextResult together into
        a single array of results. This is mostly here for historical purposes. Users
        should migrate to using the Result objects directly.
    """
    # Magic for nested key generation
    # https://stackoverflow.com/a/27809959
    collected = defaultdict(lambda: defaultdict(odict))

    # ContextResults
    for r in results:

        # Shortcut for StreamConfigResult objects when someone uses QcConfig.run() directly
        # and doesn't go through a Stream object
        if isinstance(r, StreamConfigResult):
            collected[r.package][r.test] = r.results
            continue

        # iterate over the StreamConfigResults
        for tr in r.results:
            testpackage = tr.package
            testname = tr.test
            testresults = tr.results

            if testname not in collected[r.stream_id][testpackage]:
                collected[r.stream_id][testpackage][testname] = pd.Series(
                    QartodFlags.UNKNOWN,
                    index=r.subset_indexes,
                    dtype='uint8'
                )
            collected[r.stream_id][testpackage][testname].loc[r.subset_indexes] = testresults

    return collected
