#/bin/bash
set -e
mkdir -p vendor/
pip install --exists-action=w --download vendor/ -r requirements-normal.txt
pip install --download vendor/ -r requirements-native.txt --no-use-wheel
