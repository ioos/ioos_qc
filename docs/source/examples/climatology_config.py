"""Run an offline monthly climatology config."""  # noqa: INP001

import numpy as np

from ioos_qc.config import Config
from ioos_qc.results import collect_results
from ioos_qc.streams import NumpyStream

config = Config(
    {
        "qartod": {
            "climatology_test": {
                "config": [
                    {
                        "tspan": [1, 3],
                        "period": "month",
                        "vspan": [10, 13],
                        "zspan": [0, 100],
                    },
                    {
                        "tspan": [4, 6],
                        "period": "month",
                        "vspan": [11, 14],
                        "zspan": [0, 100],
                    },
                ],
            },
        },
    },
)

# Synthetic temperatures in degrees Celsius.
one_year = [11.0, 20.0, np.nan, 12.0, 20.0, np.nan]
values = np.array(one_year * 2)
dates = []
for year in (2026, 2027):
    dates.extend(f"{year}-{month:02d}-01" for month in range(1, 7))
times = np.array(dates, dtype="datetime64[ns]")
# Metres, within both configured depth spans.
depths = np.zeros(values.size)

stream = NumpyStream(values, times, depths)
collected = collect_results(stream.run(config), how="list")
flags = collected[0].results
print(flags.tolist())
# [1, 3, 9, 1, 3, 9, 1, 3, 9, 1, 3, 9]
