import logging
import os

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

    if not file_path:  # Check if file_path is an empty string
        file_path = './'  # Default to current working directory

    full_path = os.path.join(file_path, file_name + '.csv')

    # Check if the file already exists
    if os.path.exists(full_path):
        if not overwrite:
            logging.error(
                f"File '{full_path}' already exists. Set overwrite-flag to True in order to perform this action")
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
