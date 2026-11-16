Development and Testing
=======================

Environment setup
-----------------

Create a conda environment::

    conda create -n ioosqc38 python=3.12
    conda activate ioosqc38
    python -m pip install --upgrade pip
    python -m pip install -e . --group dev

Run tests::

    pytest

Docs
-----

Build docs::

    conda create -y -n ioosqc38_docs python=3.12
    conda activate ioosqc38_docs
    python -m pip install --upgrade pip
    python -m pip install -e ".[extras]" --group dev --group docs
    cd docs
    make html

Then open a browser to ``build/html/index.html``.

*Note:* If you encounter problems during the build, delete the ``docs/source/examples/.ipynb_checkpoints/`` folder and try again.

Run Jupyter server and edit notebook examples::

    conda activate ioosqc38_docs
    cd docs/source/examples
    jupyter notebook

*Note:* If you add or edit the notebook examples, you should save the result with the *output cleared on all cells*.
This way, ``nbsphinx`` will build the notebook output during the build stage.

Releasing
---------

1. Tag `main` with the version number. That's it.
