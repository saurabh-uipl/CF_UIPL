#!/bin/bash
cd /tmp
 
echo "Copying packages from S3..."
aws s3 cp s3://asb-s3-dev-bah-lakehouse-framework-01-19trqb84/python-packages/emr-py-offline-packages.zip .

unzip emr-py-offline-packages.zip 
 
echo "Listing extracted files:"
ls -l

sudo pip3 install  \
--no-index  \
--find-links=$(pwd)/offline_packages  \
-r requirements.txt  

echo "All packages installed successfully."
