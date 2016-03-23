#!/bin/bash
# Builds an artifact that can be used in offline deployment of the application.

set -e
VENDOR=vendor/

# prepare dependencies
if [ -d $VENDOR ]; then
    rm -rf $VENDOR
fi
mkdir $VENDOR
pip install --exists-action=w --download $VENDOR -r requirements-normal.txt
pip install --download $VENDOR -r requirements-native.txt --no-use-wheel

# prepare build manifest
echo "commit_sha=$(git rev-parse HEAD)" > build_info.ini

# assemble the artifact
VERSION=$(grep current_version .bumpversion.cfg | cut -d " " -f 3)
zip -r data-catalog-${VERSION}.zip $VENDOR data_catalog/ manifest.yml requirements.txt runtime.txt build_info.ini
