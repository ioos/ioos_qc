Installation
============

Install with conda::

    $ conda install -c conda-forge ioos_qc


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

*Note:* If you add or edit the notebook examples, you should save the result with the *output cleared on all cells*.
This way, ``nbsphinx`` will build the notebook output during the build stage. If you encounter problems during the build,
delete the ``docs/source/examples/.ipynb_checkpoints/`` folder and try again.
