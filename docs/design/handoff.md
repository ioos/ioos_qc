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