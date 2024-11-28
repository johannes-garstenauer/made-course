import io
import logging
import zipfile
import pandas as pd
import requests
from retry import retry

@retry(tries=3, delay=30, logger=logging.getLogger())
def extract_dataset(dataset_url: str, timeout: (int, int) = (None, None), is_zip: bool = False):
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
            if len(csv_files) == 1:  # Ensure that there is only a singular csv dataset file
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
        # df = pd.read_csv(io.StringIO(csv_data.content.decode('utf-8')), sep=separator)
        df = pd.read_csv(io.StringIO(csv_data), sep=separator, skiprows=skiprows)
        logging.info(f"Successfully loaded data into DataFrame with {len(df)} rows")
        return df
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to load data: {e}")
        raise e
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise e
