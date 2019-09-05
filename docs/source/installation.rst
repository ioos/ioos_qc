Installation
============

Install with conda::

    $ conda install -c axiom-data-science ioos_qc


Development and Testing
-----------------------

Create a conda environment::

    conda create -n ioosqc37 python=3.7
    conda activate ioosqc37
    conda install --file requirements.txt --file tests/requirements.txt

Run tests::

    pytest


Build docs::

    cd docs
    conda activate ioosqc37
    conda install --file requirements.txt --file source/examples/requirements.txt
    make html

Then open a browser to ``build/html/index.html``.