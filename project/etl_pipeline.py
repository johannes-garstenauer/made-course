########################################################################################################################
# For all datasets:
# 1. Extract dataset: 
#     - HTTP download into csv file format
#         - Resilience: Add retries after n-seconds if unsuccessful initially
# 
# 2. Transform dataset
#     - Drop all columns that are unnecessary
#     - Check for missing values:
#         - if over a certain threshold: handle with:
#             - mean/mode/deletion/imputation
#     - Change time datatypes into datetime format
#     - Filter for required rows
# 
# 3. Load dataset into /data/ folder
# 
# - In order to make it modular (and because we are working with a large number of datasets):
#     - define functions for standard tasks
#         - download
#         - datetime transformation
#         - missing value handling
#         - dropping columns
#         - filtering rows
#         - saving dataset
# - Throughout it all use logging (on console & also in a log file? use library?) 
# - Focus on Error Handling
########################################################################################################################
########################################################################################################################
# Content Overview
# 1. Function Imports
#   1.1 For Extract Step in ETL
#   1.2 For Transform Step in ETL
#   1.3 For Load Step in ETL
# 2. Pipeline with 5 Datasets
########################################################################################################################

import time

# Import ETL functions
from extraction import *
from loading import load_df_to_csv
from transformation import *

# Configure the logging system
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

########################################################################################################################
################ PIPELINE START ########################################################################################
########################################################################################################################

# # Applying the ETL Pipeline to the datasets
skip = False # Flag indicating whether the next dataset is to be skipped, for instance, when the data download was unsuccessful.
start_time = time.time() # Measure the pipeling execution time.

# ### Chile Covid Mortality Dataset
chile_url = "https://datos.gob.cl/dataset/8982a05a-91f7-422d-97bc-3eee08fde784/resource/8e5539b7-10b2-409b-ae5a-36dae4faf817/download/defunciones_covid19_2020_2024.csv"

try:
    # Extract the dataset into a data-frame
    data = extract_dataset(chile_url, timeout=(200, 200))
    chile_df = extract_into_df(data, separator=";")
except Exception as e:
    skip = True
    logging.error("Unexpected error when loading dataset. Skipping the pipeline to the next dataset in the script.")

if skip:
    logging.warning("Skipping dataset")
    skip = False # Reset the flag indicating whether the next dataset is to be skipped.
else: # Perform transformations

    # Required fields for analysis are the death-date and the diagnosis (COVID-19)
    chile_df = filter_drop_columns(chile_df, ["FECHA_DEF", "DIAG1"])

    # Transform the date-fields into datetime objects
    chile_df = filter_transform_to_datetime(chile_df, "FECHA_DEF")

    # No missing values are imputed, as there are not enough missing values in the dataset
    for column in chile_df.columns:
        chile_df = filter_handle_missing_values(chile_df, column=column, strategy=Strategy.DROP_ROW)

    # Load the transformed dataframe back into a CSV-database file.
    load_df_to_csv(chile_df, file_name='chile_covid_mortality', overwrite=False)

# ### USA Covid Mortality Dataset
usa_url = "https://data.cdc.gov/api/views/exs3-hbne/rows.csv?fourfour=exs3-hbne&cacheBust=1729520760&date=20241106&accessType=DOWNLOAD"

# Extract the dataset into a data-frame
try:
    data = extract_dataset(usa_url, timeout=(200, 200))
    usa_df = extract_into_df(data)
except Exception as e:
    skip = True
    logging.error("Unexpected error when loading dataset. Skipping the pipeline to the next dataset in the script.")

if skip:
    logging.warning("Skipping dataset")
    skip = False # Reset the flag indicating whether the next dataset is to be skipped.
else: # Perform transformations

    # This dataset has duplicate values, therefore drop all rows for the different regions in the US and keep only the total US rows.
    usa_df = filter_rows_by_values(usa_df, "jurisdiction_residence", "United States")

    # Keep only required fields for analysis
    usa_df = filter_drop_columns(usa_df, ["data_period_start", "data_period_end", "group", "subgroup1", "covid_deaths", "crude_rate"])

    # Transform the date-fields into datetime objects. This also works for the american M/D/Y date format.
    usa_df = filter_transform_to_datetime(usa_df, "data_period_start")
    usa_df = filter_transform_to_datetime(usa_df, "data_period_end")

    # Drop the rows for which there is no data about covid mortality
    for column in usa_df.columns:
        usa_df = filter_handle_missing_values(usa_df, column=column, strategy=Strategy.DROP_ROW)

    # Load the transformed dataframe back into a CSV-database file.
    load_df_to_csv(usa_df, file_name='usa_covid_mortality', overwrite=False)

# ### Colombia Covid Mortality Dataset
colombia_url = "https://www.datos.gov.co/api/views/jp5m-e7yr/rows.csv?fourfour=jp5m-e7yr&cacheBust=1705599009&date=20241106&accessType=DOWNLOAD"

try:
    # Extract the dataset into a data-frame
    data = extract_dataset(colombia_url, timeout=(200, 200))
    colombia_df = extract_into_df(data)
except Exception as e:
    skip = True
    logging.error("Unexpected error when loading dataset. Skipping the pipeline to the next dataset in the script.")
if skip:
    logging.warning("Skipping dataset")
    skip = False # Reset the flag indicating whether the next dataset is to be skipped.
else: # Perform transformations

    # Keep only required fields for analysis
    colombia_df = filter_drop_columns(colombia_df, ["Fecha de muerte", "Recuperado"])

    # Transform the date-fields into datetime objects.
    colombia_df = filter_transform_to_datetime(colombia_df, "Fecha de muerte")

    # No missing values are imputed, as there are not enough missing values in the dataset
    for column in colombia_df.columns:
        colombia_df = filter_handle_missing_values(colombia_df, column=column, strategy=Strategy.MEDIAN)

    # Load the transformed dataframe back into a CSV-database file.
    load_df_to_csv(colombia_df, file_name='colombia_covid_mortality', overwrite=False)

# ### Mexico Covid Mortality Dataset
mexico_url = "https://datos.covid-19.conacyt.mx/Downloads/Files/Casos_Diarios_Estado_Nacional_Defunciones_20230625.csv"

try:
    # Extract the dataset into a data-frame
    data = extract_dataset(mexico_url, timeout=(200, 200))
    mexico_df = extract_into_df(data)
except Exception as e:
    skip = True
    logging.error("Unexpected error when loading dataset. Skipping the pipeline to the next dataset in the script.")

if skip:
    logging.warning("Skipping dataset")
    skip = False # Reset the flag indicating whether the next dataset is to be skipped.
else: # Perform transformations

    # Transform the date column names into datetime format
    mexico_df = filter_transform_to_datetime(mexico_df, do_columns=True)

    # Keep the nombre column and all date columns as they will all be required for the analysis.
    # For this dataset, having to use a whitelist is a bit unfortunate, however we work around this issue by generating all column names automatically.
    start_date, end_date = '17-03-2020', '23-06-2023' # Define the date range
    date_range = pd.date_range(start=start_date, end=end_date).date.tolist() # Generate the date range
    date_range.append("nombre")

    mexico_df = filter_drop_columns(mexico_df, white_list=date_range)

    # Leave only the row containing national mortality in order to avoid having duplicate values.
    mexico_df = filter_rows_by_values(mexico_df, "nombre", "Nacional")

    # No missing values need to be imputed, as there are not enough missing values in the dataset
    assert mexico_df.isnull().sum().sum() == 0

    # Load the transformed dataframe back into a CSV-database file.
    load_df_to_csv(mexico_df, file_name='mexico_covid_mortality', overwrite=False)

# ### World Population Dataset
world_pop_url = "https://api.worldbank.org/v2/en/indicator/SP.POP.TOTL?downloadformat=csv"

try:
    # Extract the dataset into a data-frame
    data = extract_dataset(world_pop_url, timeout=(200, 200), is_zip=True)  # Unzip and identify dataset
    world_pop_df = extract_into_df(data, skiprows=3)  # Skip first three rows of metadata to get usable dataframe
except Exception as e:
    skip = True
    logging.error("Unexpected error when loading dataset. Skipping the pipeline to the next dataset in the script.")

if skip:
    logging.warning("Skipping dataset")
    skip = False # Reset the flag indicating whether the next dataset is to be skipped.
else: # Perform transformations

    # Keep the data for the years 2020-2023 and the country name as an identifier
    white_list = [str(x) for x in range(2020, 2024)]
    white_list.append("Country Name")
    world_pop_df = filter_drop_columns(world_pop_df, white_list)

    # Select rows for the countries under analysis
    countries = ["Chile", "United States", "Colombia", "Mexico"]
    world_pop_df = filter_rows_by_values(world_pop_df, "Country Name", countries)

    # No missing values are imputed, as there are not enough missing values in the dataset
    assert world_pop_df.isnull().sum().sum() == 0

    # Load the transformed dataframe back into a CSV-database file.
    load_df_to_csv(world_pop_df, file_name='world_population_total', overwrite=False)

end_time = time.time() # Measure pipeline execution time.
elapsed_time = end_time - start_time # Calculate elapsed time
logging.info(f"Pipeline finished in {elapsed_time:.2f} seconds")