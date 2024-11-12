#!/bin/bash

#######README#############

# Potentially you will have to make this script executable first
# Use 'chmod +x path_to_repo/project/pipeline.sh
# Make sure to replace path_to_repo with the path to your repo

# Make sure to install the following packages to run this script successfully:
# pandas
# retry
# requests
# numpy

# Note: Unfortunately the Data Portal providing the Chilean dataset is currently offline.
# As a result the code for this dataset has been removed from execution.

#######README END########

# Execute the Python script
python3 etl_pipeline.py