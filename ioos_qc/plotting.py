import logging
import numpy as np

from bokeh import plotting
from bokeh.layouts import gridplot

L = logging.getLogger(__name__)


def bokeh_plot(data, var_name, results, title, module, test_name):
    plot = bokeh_plot_var(data, var_name, results, title, module, test_name)
    return plotting.show(gridplot([[plot]], sizing_mode='fixed'))


def bokeh_plot_var(data, var_name, results, title, module, test_name):
    """ Method to plot QC results using Bokeh """

    time = data.index
    obs = data[var_name]
    if module not in results or test_name not in results[module]:
        L.warning(f'No results for test {module}.{test_name} found')
        return

    qc_test = results[module][test_name]

    qc_pass = np.ma.masked_where(qc_test != 1, obs)
    qc_suspect = np.ma.masked_where(qc_test != 3, obs)
    qc_fail = np.ma.masked_where(qc_test != 4, obs)
    qc_notrun = np.ma.masked_where(qc_test != 2, obs)

    p1 = plotting.figure(x_axis_type="datetime", title=test_name + ' : ' + title)
    p1.grid.grid_line_alpha = 0.3
    p1.xaxis.axis_label = 'Time'
    p1.yaxis.axis_label = 'Observation Value'

    p1.line(time, obs,  legend_label='obs', color='#A6CEE3')
    p1.circle(time, qc_notrun, size=2, legend_label='qc not run', color='gray', alpha=0.2)
    p1.circle(time, qc_pass, size=4, legend_label='qc pass', color='green', alpha=0.5)
    p1.circle(time, qc_suspect, size=4, legend_label='qc suspect', color='orange', alpha=0.7)
    p1.circle(time, qc_fail, size=6, legend_label='qc fail', color='red', alpha=1.0)
    p1.circle(time, qc_notrun, size=6, legend_label='qc not eval', color='gray', alpha=1.0)

    return p1


def bokeh_multi_plot(data, results, title, **kwargs):

    kwargs = {
        **{
            'merge_tools': True,
            'toolbar_location': 'below',
            'sizing_mode': 'scale_width',
            'plot_width': 600,
            'plot_height': 200,
            'ncols': 2
        },
        **kwargs
    }

    plots = list(bokeh_multi_var(data, results, title))
    return plotting.show(gridplot(plots, **kwargs))


def bokeh_multi_var(data, results, title):
    for vname, qcobj in results.items():
        for modu, tests in qcobj.items():
            for testname, testresults in tests.items():
                plt = bokeh_plot_var(data, vname, qcobj, title, modu, testname)
                yield plt
