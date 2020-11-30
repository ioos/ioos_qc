import logging
import numpy as np

from bokeh import plotting
from bokeh.layouts import gridplot

L = logging.getLogger(__name__)


def bokeh_plot(data, var_name, results, title, module, test_name):
    plot = bokeh_plot_var(data, var_name, results, title, module, test_name)
    return gridplot([[plot]], sizing_mode='fixed')


def bokeh_plot_var(time, data, var_name, results, title, module, test_name):
    """ Method to plot QC results using Bokeh """

    if module not in results or test_name not in results[module]:
        L.warning(f'No results for test {module}.{test_name} found')
        return

    qc_test = results[module][test_name]

    qc_pass = np.ma.masked_where(qc_test != 1, data)
    qc_suspect = np.ma.masked_where(qc_test != 3, data)
    qc_fail = np.ma.masked_where(qc_test != 4, data)
    qc_notrun = np.ma.masked_where(qc_test != 2, data)

    p1 = plotting.figure(x_axis_type="datetime", title=test_name + ' : ' + title)
    p1.grid.grid_line_alpha = 0.3
    p1.xaxis.axis_label = 'Time'
    p1.yaxis.axis_label = 'Data'

    p1.line(time, data,  legend_label='data', color='#A6CEE3')
    p1.circle(time, qc_notrun, size=2, legend_label='qc not run', color='gray', alpha=0.2)
    p1.circle(time, qc_pass, size=4, legend_label='qc pass', color='green', alpha=0.5)
    p1.circle(time, qc_suspect, size=4, legend_label='qc suspect', color='orange', alpha=0.7)
    p1.circle(time, qc_fail, size=6, legend_label='qc fail', color='red', alpha=1.0)
    p1.circle(time, qc_notrun, size=6, legend_label='qc not eval', color='gray', alpha=1.0)

    return p1


def bokeh_multi_plot(stream, results, title, **kwargs):

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

    plots = list(bokeh_multi_var(stream, results, title))
    return gridplot(plots, **kwargs)


def bokeh_multi_var(stream, results, title):
    for vname, qcobj in results.items():
        for modu, tests in qcobj.items():
            for testname, testresults in tests.items():
                plt = bokeh_plot_var(stream.time(), stream.data(vname), vname, qcobj, title, modu, testname)
                yield plt


def bokeh_plot_collected_results(results, **kwargs):
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

    plots = []
    for r in results:
        if r.data.any() and r.results.any():
            plots.append(bokeh_plot_collected_result(r))
    return gridplot(plots, **kwargs)


def bokeh_plot_collected_result(cr):
    title = f'{cr.stream_id}: {cr.package}.{cr.test}'
    p1 = plotting.figure(x_axis_type="datetime", title=title)
    p1.grid.grid_line_alpha = 0.3
    p1.xaxis.axis_label = 'Time'
    p1.yaxis.axis_label = 'Data'
    qc_pass = np.ma.masked_where(cr.results != 1, cr.data)
    qc_suspect = np.ma.masked_where(cr.results != 3, cr.data)
    qc_fail = np.ma.masked_where(cr.results != 4, cr.data)
    qc_notrun = np.ma.masked_where(cr.results != 2, cr.data)

    p1.line(cr.tinp, cr.data,  legend_label='data', color='#A6CEE3')
    p1.circle(cr.tinp, qc_notrun, size=3, legend_label='qc not run', color='gray', alpha=0.2)
    p1.circle(cr.tinp, qc_pass, size=4, legend_label='qc pass', color='green', alpha=0.5)
    p1.circle(cr.tinp, qc_suspect, size=4, legend_label='qc suspect', color='orange', alpha=0.7)
    p1.circle(cr.tinp, qc_fail, size=6, legend_label='qc fail', color='red', alpha=1.0)
    p1.circle(cr.tinp, qc_notrun, size=3, legend_label='qc not eval', color='gray', alpha=1.0)

    return p1
