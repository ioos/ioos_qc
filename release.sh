#!/bin/bash

if [ $# -eq 0 ]; then
    echo "No version specified, exiting"
    exit 1
fi

# Set version to release
sed -i "s/^__version__ = .*/__version__ = \"$1\"/" ioos_qartod/__init__.py
sed -i "s/version: .*/version: \"$1\"/" conda-recipe/meta.yaml
sed -i "s/version = .*/version = \"$1\"/" docs/source/conf.py
sed -i "s/release = .*/release = \"$1\"/" docs/source/conf.py
echo $1 > VERSION

# Commit release
git add ioos_qartod/__init__.py
git add conda-recipe/meta.yaml
git add VERSION
git add docs/source/conf.py
git commit -m "Release $1"

# Tag
git tag $1

echo "Now push this branch and tag!"
