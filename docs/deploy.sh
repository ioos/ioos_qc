#!/bin/bash
set -ev

cd docs
conda install --file requirements.txt
#sphinx-apidoc -M -f -o source/api ../ioos_qc
make html
cd ..
doctr deploy . --built-docs docs/build/html/
