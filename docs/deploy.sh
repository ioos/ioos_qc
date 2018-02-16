#!/bin/bash
set -ev

cd docs
conda install --file requirements.txt
make html
cd ..
doctr deploy . --built-docs docs/build/html/
