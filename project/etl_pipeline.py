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

import pandas as pd
import logging
from retry import retry
import requests	
from enum import Enum, auto
import io
import copy
import zipfile
import os

# Configure the logging system
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@retry(tries=3, delay=30, logger=logging.getLogger())
def extract_dataset(dataset_url: str, timeout: (int,int) = (None, None), is_zip: bool = False):
    """
    Download datasets via HTTP request.
    Retry three times, after waiting for 30s each, if unsuccessful.
    
    Parameters:
    dataset_url: (str): URL of a dataset in the csv-file format.
    timeout: (int, int): The timeout for the HTTP request in seconds. First tuple value is connection timeout, second tuple value is read timeout. Default behaviour is, that no time-out is applied
    is_zip: (bool): Flag indicating if the dataset is a zip file containing multiple CSV files.
    
    Returns:
    response: The decoded response content
    """

    logging.info(f"Attempting to fetch data from {dataset_url}") 
    response = requests.get(dataset_url, timeout=timeout) 
    response.raise_for_status()  # Raise an exception for HTTP errors 
    logging.info(f"Successfully fetched data from {dataset_url}") 
    
    # Pick out the csv dataset file which is not metadata (as identified by file-name).
    if is_zip:
        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
            csv_files = [f for f in zip_file.namelist() if f.endswith('.csv') and 'metadata' not in f.lower()]
            if len(csv_files) == 1: # Ensure that there is only a singular csv dataset file
                with zip_file.open(csv_files[0]) as csv_file:
                    return csv_file.read().decode('utf-8')
            else:
                raise ValueError(f"Expected exactly one CSV file without 'metadata' in the name, found: {csv_files}")
    else:
        return response.content.decode('utf-8')

def extract_into_df(csv_data, separator=",", skiprows=0):
    """
    Load a csv dataset into a pandas dataframe for further transformation.
    
    Parameters:
    csv_data: Dataset in CSV format as provided by extract_dataset_function.
    separator: The CSV value separator for this file
    skiprows: The number of rows of metadata that are to be skipped at the beginning of the file
    
    Returns:
    pd.DataFrame: The resulting DataFrame
    """
    
    try: 
        logging.info(f"Attempting to load data into DataFrame") 
        #df = pd.read_csv(io.StringIO(csv_data.content.decode('utf-8')), sep=separator)
        df = pd.read_csv(io.StringIO(csv_data), sep=separator, skiprows=skiprows)
        logging.info(f"Successfully loaded data into DataFrame with {len(df)} rows") 
        return df 
    except requests.exceptions.RequestException as e: 
        logging.error(f"Failed to load data: {e}") 
        raise e 
    except Exception as e: 
        logging.error(f"Unexpected error: {e}") 
        raise e

def filter_drop_columns(df, white_list):
    """
    Drop columns from a DataFrame except those in the whitelist.
    
    Parameters:
    df (pd.DataFrame): The DataFrame to operate on.
    white_list (list): List of columns to keep.
    
    Returns:
    pd.DataFrame: DataFrame with only the columns in the whitelist.
    """
    
    try:
        logging.info(f"Attempting to drop non-whitelisted columns")
        
        # Check if whitelist columns exist in DataFrame
        missing_columns = [col for col in white_list if col not in df.columns]
        if missing_columns:
            raise ValueError(f"The following columns in the whitelist are missing from the DataFrame: {missing_columns}")
        
        # Drop columns not in the whitelist
        columns_to_drop = [col for col in df.columns if col not in white_list]
        df_dropped = df.drop(columns=columns_to_drop)
        
        if len(list(df_dropped.columns)) < 15:
            logging.info(f"Successfully dropped columns. Remaining columns: {list(df_dropped.columns)}")
        else:
            logging.info(f"Successfully dropped columns. {len(list(df_dropped.columns))} columns remaining.")
            
        return df_dropped
    
    except ValueError as e:
        logging.error(f"Please make sure all columns in the white list are contained in the DataFrame: {e}")
        return df
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return df


class Strategy(Enum):
    """
    Each enumeration represents a strategy for handling missing values of pd.DataFrame
    """
    
    BFILL = auto()
    FFILL = auto()
    DROP_ROW = auto()
    LINEAR_INTERPOLATION = auto()
    MODE = auto()
    MEDIAN = auto()

def filter_handle_missing_values(df, column, threshold=0, strategy=Strategy.DROP_ROW):
    """
    Handle missing values in a specific column of a DataFrame based on a given strategy.
    
    Parameters:
    df (pd.DataFrame): The DataFrame to operate on.
    column (str): The column to handle missing values for.
    threshold (float): Threshold of missing values (0-1) after which the strategy is applied.
    strategy (Strategy): Strategy for handling missing values.
    
    Returns:
    pd.DataFrame: DataFrame with missing values handled in the specified column.
    """
    
    try:
        temp_df = copy.deepcopy(df) # Avoid making in place changes to the dataframe
        
        # Calculate the percentage of missing values in the column
        missing_ratio = temp_df[column].isnull().mean()
        
        if missing_ratio > threshold:
            logging.info(f"Column '{column}' has {missing_ratio * 100:.2f}% missing values, applying {strategy.name} strategy")
            
            if strategy == Strategy.BFILL:
                temp_df[column] = temp_df[column].fillna(method='bfill')
                logging.info(f"Applied back fill strategy to column '{column}'")
            
            elif strategy == Strategy.FFILL:
                temp_df[column] = temp_df[column].fillna(method='ffill')
                logging.info(f"Applied forward fill strategy to column '{column}'")
            
            elif strategy == Strategy.DROP_ROW:
                temp_df = temp_df.dropna(subset=[column])
                logging.info(f"Dropped rows with missing values in column '{column}'")
            
            elif strategy == Strategy.LINEAR_INTERPOLATION:
                temp_df[column] = temp_df[column].interpolate(method='linear')
                logging.info(f"Applied linear interpolation to column '{column}'")
            
            elif strategy == Strategy.MODE:
                mode_value = temp_df[column].mode()[0]
                temp_df[column] = temp_df[column].fillna(mode_value)
                logging.info(f"Applied mode imputation to column '{column}' with mode value {mode_value}")
            
            elif strategy == Strategy.MEDIAN:
                median_value = temp_df[column].median()
                temp_df[column] = temp_df[column].fillna(median_value)
                logging.info(f"Applied median imputation to column '{column}' with median value {median_value}")
            
            return temp_df
        else:
            logging.info(f"Column '{column}' has {missing_ratio * 100:.2f}% missing values, which is lower than the threshold of {threshold * 100:.2f}%. Not applying a strategy.")
            return df
    
    except Exception as e:
        logging.error(f"Unexpected error while handling missing values in column '{column}' with strategy '{strategy.name}': {e}")
        return df

def filter_rows_by_values(df, column_name, column_values):
    """
    Filter rows in a DataFrame based on column values and drop all rows where values do not match the given values.
    
    Parameters:
    df (pd.DataFrame): The DataFrame to filter.
    column_name (str): The column name to check for the values.
    column_values: The value or list of values to filter rows by.
    
    Returns:
    pd.DataFrame: DataFrame with rows filtered by the given column values.
    """
    
    try:
        # Ensure the column exists in the DataFrame
        if column_name not in df.columns:
            logging.error(f"Column '{column_name}' does not exist in the DataFrame.")
            return df
        
        # If column_values is not a list, convert it to a list
        if not isinstance(column_values, list):
            column_values = [column_values]
        
        # Create a mask for the matching rows
        mask = df[column_name].isin(column_values)
        affected_rows = len(df) - mask.sum()  # Calculate the number of rows that do not match
        
        # Filter the DataFrame
        filtered_df = df[mask]
        
        logging.info(f"Column '{column_name}': Filtering rows where values are in {column_values}.")
        logging.info(f"Number of rows dropped: {affected_rows}")
        return filtered_df
    except Exception as e:
        logging.error(f"Unexpected error while filtering rows by '{column_name}' with value '{column_values}': {e}")
        return df

# Helper function to check if a column name can be converted to a datetime object
# Returns a date object, if a conversion is possible, otherwise returns the column name
def try_convert_to_datetime(col_name):
    try:
        return pd.to_datetime(col_name, dayfirst=True, errors='raise').date()
    except ValueError:
        return col_name


def filter_transform_to_datetime(df, column=None, do_columns=False):
    """
    Transform a column in various formats to a uniform datetime datatype.
    Alternatively transform column names to datetime datatype.
    
    Parameters:
    df (pd.DataFrame): The DataFrame containing the date column.
    column (str): The column name containing date values in various formats.
    doColumns (bool): If true, all column names that represent a date will be transformed to uniform datetime objects
    
    Returns:
    pd.DataFrame: DataFrame with the date column transformed to datetime.
    """
    
    try:
        
        temp_df = copy.deepcopy(df) # Avoid making in place changes to the dataframe
        if do_columns:
            
            # Transform columns that can be parsed as datetime objects
            new_columns = {col: try_convert_to_datetime(col) for col in temp_df.columns}
            temp_df.rename(columns=new_columns, inplace=True)
            logging.info(f"Successfully transformed {len(new_columns)} column names to datetime")
        elif column is not None:
            logging.info(f"Transforming column '{column}' to datetime")
            temp_df[column] = pd.to_datetime(temp_df[column], errors='coerce').dt.date
            logging.info(f"Successfully transformed column '{column}' to datetime")
        else:
            logging.error("Please provide an existing column name")
    except Exception as e:
        logging.error(f"Unexpected error while transforming column '{column}' to datetime: {e}")
        return df
    
    return temp_df

def load_df_to_csv(df, file_name, file_path='../data/', overwrite=False):
    """
    Save a DataFrame to a CSV file.
    
    Parameters:
    df (pd.DataFrame): The DataFrame to save.
    file_path (str): The path where the CSV file will be saved. The default path is that to the local /data/ folder, as required by the project specifications.
    file_name (str): The name of the file to be stored, excluding the file ending, which is hardcoded as '.csv'
    overwrite (bool): Flag to allow overwriting of existing files.
    
    Returns:
    None
    """
    
    if not file_path: # Check if file_path is an empty string 
        file_path = './' # Default to current working directory 
    
    full_path = os.path.join(file_path, file_name + '.csv')
    
    # Check if the file already exists 
    if os.path.exists(full_path):
        if not overwrite:
            logging.error(f"File '{full_path}' already exists. Set overwrite-flag to True in order to perform this action") 
            return
        else: 
            logging.warning(f"File '{full_path}' is being overwritten as the overwrite-flag is set to True")
        
    
    try:
        df.to_csv(full_path, index=False)
        logging.info(f"DataFrame successfully saved to {full_path}")
    except PermissionError as e:
        logging.error(f"Permission error while trying to save the DataFrame to {full_path}: {e}")
    except FileNotFoundError as e:
        logging.error(f"File not found error while trying to save the DataFrame to {full_path}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error while saving the DataFrame to {full_path}: {e}")

# # Applying the ETL Pipeline to the datasets 
# ### Chile Covid Mortality Dataset

'''
chile_url = "https://datos.gob.cl/dataset/8982a05a-91f7-422d-97bc-3eee08fde784/resource/8e5539b7-10b2-409b-ae5a-36dae4faf817/download/defunciones_covid19_2020_2024.csv"

# Extract the dataset into a data-frame
data = extract_dataset(chile_url, timeout=(200,200))
chile_df = extract_into_df(data, separator=";")

# Perform transformations
# Required fields for analysis are the death-date and the diagnosis (COVID-19)
chile_df = filter_drop_columns(chile_df, ["FECHA_DEF", "DIAG1"])

# Transform the date-fields into datetime objects
chile_df = filter_transform_to_datetime(chile_df, "FECHA_DEF")

# No missing values are imputed, as there are not enough missing values in the dataset
for column in chile_df.columns:
    chile_df = filter_handle_missing_values(chile_df, column=column, strategy=Strategy.DROP_ROW)

# Load the transformed dataframe back into a CSV-database file.
load_df_to_csv(chile_df, file_name='chile_covid_mortality', overwrite=False)
'''

# ### USA Covid Mortality Dataset
usa_url = "https://data.cdc.gov/api/views/exs3-hbne/rows.csv?fourfour=exs3-hbne&cacheBust=1729520760&date=20241106&accessType=DOWNLOAD"

# Extract the dataset into a data-frame
data = extract_dataset(usa_url, timeout=(200, 200))
usa_df = extract_into_df(data)

# Perform transformations

# This dataset has duplicate values, therefore drop all rows for the different regions in the US and keep only the total US rows.
usa_df = filter_rows_by_values(usa_df, "jurisdiction_residence", "United States")

# Keep only required fields for analysis
usa_df = filter_drop_columns(usa_df, ["data_period_start", "data_period_end", "group", "subgroup1", "covid_deaths", "crude_rate"]) 

# Transform the date-fields into datetime objects. This also works for the american M/D/Y date format.
usa_df = filter_transform_to_datetime(usa_df, "data_period_start") 
usa_df = filter_transform_to_datetime(usa_df, "data_period_end")

# Drop the rows for which there is no data about covid mortality
print(f"Before: \n{usa_df.isnull().sum()}\n")
for column in usa_df.columns:
    usa_df = filter_handle_missing_values(usa_df, column=column, strategy=Strategy.DROP_ROW)
print(f"After: \n{usa_df.isnull().sum()}\n")

# Load the transformed dataframe back into a CSV-database file.
load_df_to_csv(usa_df, file_name='usa_covid_mortality', overwrite=False)

# ### Colombia Covid Mortality Dataset

colombia_url = "https://www.datos.gov.co/api/views/jp5m-e7yr/rows.csv?fourfour=jp5m-e7yr&cacheBust=1705599009&date=20241106&accessType=DOWNLOAD"

# Extract the dataset into a data-frame
data = extract_dataset(colombia_url, timeout=(200, 200))
colombia_df = extract_into_df(data)

# Perform transformations

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

# Extract the dataset into a data-frame
data = extract_dataset(mexico_url, timeout=(200, 200))
mexico_df = extract_into_df(data)

# Perform transformations

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

# Extract the dataset into a data-frame
data = extract_dataset(world_pop_url, timeout=(200, 200), is_zip=True) # Unzip and identify dataset
world_pop_df = extract_into_df(data, skiprows=3) # Skip first three rows of metadata to get usable dataframe

# Perform transformations
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
load_df_to_csv(colombia_df, file_name='world_population_total', overwrite=False)