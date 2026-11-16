Development and Testing
=======================

Environment setup
-----------------

We suggest conda but any Python installer would do as long as the `dev` and `docs` dependency groups are installed.
Here is an example on how to create and environment for Python 3.14.

Create a conda environment::

    conda env create --name IOOSQC "python=3.14" pip
    conda activate IOOSQC
    pip install --group dev
    pip install --group docs
    pip install -e ".[extras]"

Then run the tests with:

    python -m pytest -s -rxs -v

When ready, submit a human-made code change via a Pull Request that will run the full test matrix.
