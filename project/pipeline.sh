#!/bin/bash

#######README#############

# Potentially you will have to make this script executable first
# Use 'chmod +x path_to_repo/project/pipeline.sh
# Make sure to replace path_to_repo with the path to your repo

# Please be aware: The download for the chilean dataset has failed from the uni network before.
# If this issue occurs, execute from another IP address or network.

# Make sure to install the following packages to run this script successfully:
# pandas
# retry
# requests
# numpy

#######README END########

# Execute the Python script
python3 etl_pipeline.py