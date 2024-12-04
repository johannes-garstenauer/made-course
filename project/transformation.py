import copy
import logging
from enum import Enum, auto
import pandas as pd

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
            raise ValueError \
                (f"The following columns in the whitelist are missing from the DataFrame: {missing_columns}")

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
            logging.info \
                (f"Column '{column}' has {missing_ratio * 100:.2f}% missing values, applying {strategy.name} strategy")

            if strategy == Strategy.BFILL:
                temp_df[column] = temp_df[column].bfill()
                logging.info(f"Applied back fill strategy to column '{column}'")

            elif strategy == Strategy.FFILL:
                temp_df[column] = temp_df[column].ffill()
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
            logging.info \
                (f"Column '{column}' has {missing_ratio * 100:.2f}% missing values, which is lower than the threshold of {threshold * 100:.2f}%. Not applying a strategy.")
            return df

    except Exception as e:
        logging.error \
            (f"Unexpected error while handling missing values in column '{column}' with strategy '{strategy.name}': {e}")
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

def _try_convert_to_datetime(column_name):
    """
    Helper function to check whether a column name can be converted to a datetime object.
    Returns a date object, if a conversion is possible, otherwise returns the column name.

    Parameters:
    column_name (str): The column name to be checked for conversion into a datetime object.

    Returns:
    pd.DataTime/str: A date object, if a conversion is possible, otherwise returns the column name.
    """
    try:
        return pd.to_datetime(column_name, dayfirst=True, errors='raise').date()
    except ValueError:
        return column_name


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
            new_columns = {col: _try_convert_to_datetime(col) for col in temp_df.columns}
            temp_df.rename(columns=new_columns, inplace=True)
            logging.info(f"Successfully transformed {len(new_columns)} column names to datetime")
        elif column is not None:
            logging.info(f"Transforming column '{column}' to datetime")

            # Suppress unnecessary warning here
            import warnings
            warnings.filterwarnings("ignore", category=UserWarning)
            temp_df[column] = pd.to_datetime(temp_df[column], errors='coerce').dt.date

            temp_df[column] = pd.to_datetime(temp_df[column], errors='coerce').dt.date
            logging.info(f"Successfully transformed column '{column}' to datetime")
        else:
            logging.error("Please provide an existing column name")
    except Exception as e:
        logging.error(f"Unexpected error while transforming column '{column}' to datetime: {e}")
        return df

    return temp_df