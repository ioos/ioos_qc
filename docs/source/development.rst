Development and Testing
=======================

Environment setup
-----------------

Create a conda environment::

    conda create -n ioosqc37 python=3.7
    conda activate ioosqc37
    conda install --file requirements.txt --file tests/requirements.txt

Run tests::

    pytest

Docs
-----

Build docs::

    conda create -y -n ioosqc37_docs python=3.7
    conda activate ioosqc37_docs
    conda install -y --file requirements.txt \
        --file tests/requirements.txt \
        --file docs/requirements.txt \
        --file docs/source/examples/requirements.txt
    cd docs
    make html

Then open a browser to ``build/html/index.html``.

*Note:* If you encounter problems during the build, delete the ``docs/source/examples/.ipynb_checkpoints/`` folder and try again.

Run Jupyter server and edit notebook examples::

    conda activate ioosqc37_docs
    cd docs/source/examples
    jupyter notebook

*Note:* If you add or edit the notebook examples, you should save the result with the *output cleared on all cells*.
This way, ``nbsphinx`` will build the notebook output during the build stage.

Releasing
---------

1. Update the version numbers in the repo
  * Look at a previous commit, e.g., `bump to 1.0.0 <https://github.com/ioos/ioos_qc/commit/e54b5e7659e632da1bbc00b7a91056f71e22512e>`_, to make sure you get all the right places.
1. Tag `master` with the version number
  * You can use the Github UI at https://github.com/ioos/ioos_qc/releases to do this easily
1. Update the conda-forge feedstock
  * Fork https://github.com/conda-forge/ioos_qc-feedstock and follow the instructions
