<!-- The goal:
* Create a trained machine learning model that is able to ID min and max ranges for the gross_range_test in ioos_qc.qartod.py.
* This will reside inside of a new module called auto_thresh.py, which will include traiing code for reference and reuse. It will save the params/weights/coefficients as a json such that they can be imported and passed into another function which the user can plug in with their own data and get model-estimates for the data parameter.

The first training dataset is here:
* https://erddap.secoora.org/erddap/tabledap/belhaven-nc.html?time%2Cwater_surface_above_navd88%2Cwater_surface_above_navd88_qc_agg%2Cz&time%3E%3D2024-03-06T20%3A18%3A15Z&time%3C%3D2026-11-17T20%3A24%3A00Z

This has one key deliverable, the auto_thresh.py module in `ioos_qc`, which includes the function used for training a model (generating coefficients/weights that can be exported and loaded) and the range_thresh() finding function, which can import said coeffs or weights and then estimate what the limits should be for a user-provided set of data.

Plan for today:
* Create the module
* Pick a model that best works with the training dataset and the objective of the module
* Run the wofkrlow once
    * Train the model, using the training data, getting coefficients
    * Run the range_thresh() trained model with some new data (which I will provide later) and the coeffs to provide estimated limits
    * Run the `gross_range_test` using the new limit estimates and compare to what we'd originally have.

At the end of the day, I want to save what I've learned into `/docs/design/intial_learning.md`, describing the key aspects of the different ML models, considerations for the training dataset, notes on usage, and some of the results. Ideally, this will also start looking at upscaling. -->

The goal:
* Create a new module, `build_limits.py`, which lives in `ioos_qc`.
* Within that module, create a function that will identify `qartod.gross_range_test` suspect bounds based on a user-specified number of standard deviations from the data. Default this to 2 stdevs. This should create an upper and a lower bound for the data. Call this function `auto_thresh()`, which only requires a specific data variable. So like, temperature or water level, for example. I do not want it to get my fail bounds, as those should be informed/supplied by the sensor manufacturer or experts.
* There will be a `climatology_config` constructor function, which will use the `auto_thresh` on different regions of the data, idetified using machine learning or clustering algorithms. For example, it should be able to identify spring from summer, or if there is a storm that rolls through. It will group these data differently, or extract dates/depths where these things occur, and then build the climatology dictionary using the suspect bounds from `auto_thresh` on that specific chunk of data.
* Use my practice dataset to build `gross_range_test` and `climatology` bounds, then run them through the quality control.
* Include a plotting function that can visualize the `gross_range_test` or `climatology` bounds on the data. If there are quality control metrics already included for that variable, also include a confusion matrix and report the F1 score to help inform the user of how well their statistically-informed `gross_range` or `climatology` bounds compare to the manually quality controlled data.

I have a practice dataset available here:
* https://erddap.secoora.org/erddap/tabledap/belhaven-nc.html?time%2Cwater_surface_above_navd88%2Cwater_surface_above_navd88_qc_agg%2Cz&time%3E%3D2024-03-06T20%3A18%3A15Z&time%3C%3D2026-11-17T20%3A24%3A00Z

Plan for today:
* Create the module
* Pick a clustering model that works best with the training dataset and the objective of the module
* Run the workflow once
    * Create gross range bounds
    * Build a climatology dictionary of different timeframes with appropriate ranges
    * Plot up all the results, reporting the F1 score if there are QC available

At the end of the day, I want to save what I've learned into `docs/design/initial_learning.md`, describing the outcomes of the workflow, notes on usage, and future results.

---

## Day 2 update

We ran an ML processor on our example data, but realised we don't need ML at all for the `gross_range_test`. It's common practice to set the global range limits to a certain number of standard deviations when there is no obvious literature describing the expected annual conditions for the data.

We re-evaluated what we thought would be required:
* Operators still don't quite know what to set their ranges to for the `gross_range_test`, `climatology_test`, or `spike_test`. These can be satisfied by:
    * Fail parameters being specified by sensor manufacturers
    * Suspect parameters being identified statistically from the data
* Furthermore, the `climatology_test` configuration has historically been very hard to define, and can be daunting for new operators or data access centers.
    * Can we, using ML clustering methods, identify different periods of time within the data (including "seasonal" or repeating trends)?
    * Run our statistical bounds during each period and build a climatology configuration through reproducible statistics.

We (including Claude) put together a new module, `build_limits.py`, which seeks to address this:
* Use clustering to identify different periods by:
    * a specified number of temporal bins (`method="segments"`, contiguous Ward clustering; ignores annual trends, better for shorter datasets),
    * shared regimes (`method="regimes"`, k-means; e.g. supporting storms in both winter and summer),
    * "seasons" (`method="seasonal"`, k-means; annual trends at a specified resolution, pooled over multiple years).
* Pass in the `auto_thresh` function with a specified number of standard deviations to permit.
* Generate the climatology config, using terminology consistent with ioos_qc's existing code (`period`, for example, to support `weekofyear`).

We passed it over our previous training set, and it successfully builds statistically reproducible bounds and configs for the `gross_range_test` and `climatology_test`.

Details, results and figures: [initial_learning.md](initial_learning.md).

### Objectives for tomorrow

Continue to refine the code and build some documentation. Ideally, we'll have the following defaults:
* Cluster my data "seasonally", such that I can build a climatology for every January, or every spring, or every 01-15 to 02-20.
    * "Seasonal" limits are the best defined for NRT data or streams, where the current time is known. This is most intuitive for people who are not experts in the region.
* Get averages for those specified ranges with a standard deviation of 3.
* Return the climatology and support plotting, so the user can see what the configuration looks like.
* Make `plot_limits` more generic, so a user can more easily visualise premade configurations for the climatology and gross range.
* Investigate `spike_test`.
* Run again with a remote participant, submitting data with known storm data.
* Finalise a Jupyter notebook that can live in the ioos_qc docs to demonstrate the workflow.

### Current state (end of Day 2, for picking up tomorrow)

* Code: `ioos_qc/build_limits.py`, tests in `tests/test_build_limits.py` (18 passing).
* Current defaults that differ from tomorrow's goals: `method="segments"` (goal: `"seasonal"`) and `n_std=2` in both `auto_thresh` and `climatology_config` (goal: 3).
* `plot_limits` already accepts any gross range span or climatology config (list of window dicts), including premade ones. It does not yet accept a legacy `ClimatologyConfig` object, a `fail_span` drawn per climatology window, or a config with `zspan` without passing `z`.
* Practice run: `PYTHONPATH=. python docs/design/scripts/run_belhaven.py` (the `qc` env's installed ioos_qc is not editable).
* Known existing ioos_qc issue: `qartod.climatology_test` fails on a tz-aware `DatetimeIndex`; pass a pandas Series or naive UTC times.
