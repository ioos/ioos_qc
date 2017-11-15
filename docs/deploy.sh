#!/bin/bash
set -ev

cd docs
conda install --file requirements.txt
pip install sphinx_autodoc_typehints
make html
cd ..
doctr deploy . --built-docs docs/build/html/
