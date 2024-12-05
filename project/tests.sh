#!/bin/bash

#######README#############

# Actions:
# Execute unit tests
# Perform system test on ETL Pipeline
# Validate output databases
# Remove datasets before and after system test

# Potentially you will have to make this script executable first
# Use 'chmod +x path_to_repo/project/tests.sh'
# Make sure to replace 'path_to_repo' with the path to your repository.

# Make sure to install the following packages to run this script successfully:
# - unittest
# - pandas
# - retry
# - requests
# - numpy

#######README END########

# Path to database files
OUTPUT_DATABASES=("../data/chile_covid_mortality.csv" "../data/colombia_covid_mortality.csv" "../data/usa_covid_mortality.csv" "../data/world_population_total.csv" "../data/mexico_covid_mortality.csv")

# Display an error message and exit
error_display() {
    echo "$1" 1>&2
    exit 1
}

# Removing datasets before and after system test
cleanup_system_test() {
    echo "Removing datasets for a clean test environment..."

    for prev_dataset in "${OUTPUT_DATABASES[@]}"; do
      if [ -f "$prev_dataset" ]; then
            rm "$prev_dataset"
            echo "Removed dataset: $prev_dataset"
        else
            echo "Dataset to remove not found: $prev_dataset"
        fi
    done

    echo "-------------------------------------------------------------------------"
}

run_unit_tests() {
    echo "Executing unit tests..."
    PYTHONPATH=$(pwd)/.. python3 -m unittest discover -s test -p 'unit_tests.py' || error_display "Unit tests failed."
    echo "-------------------------------------------------------------------------"
}

run_pipeline() {
    echo "--------------------------------------------------------------------------"
    echo "Executing the complete ETL pipeline..."
    python3 etl_pipeline.py || error_display "Pipeline execution failed."
}

validate_output() {
    echo "--------------------------------------------------------------------------"
    echo "Validating output databases..."
    for dataset in "${OUTPUT_DATABASES[@]}"; do
        if [ -f "$dataset" ]; then
            echo "Output file successfully created: $dataset"
        else
            error_display "Output dataset not found: $dataset"
        fi
    done
}

main() {
    echo "FOUND"
    run_unit_tests
    cleanup_system_test
    run_pipeline
    validate_output
    cleanup_system_test

    echo "Passed all tests successfully."
}

main