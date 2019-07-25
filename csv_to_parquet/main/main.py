import os
import pandas as pd


INPUT_DATA_DIR = f'{os.getcwd()}/data/'
OUTPUT_DATA_DIR = f'{os.getcwd()}/output_data/'

def get_file_names(path=INPUT_DATA_DIR):
    return [name for name in os.listdir(path) if '.csv' in name]

def convert_csv_to_parquet(file_name, input_dir=INPUT_DATA_DIR, output_dir=OUTPUT_DATA_DIR):
    """
    Converts given csv file into parquet format.
    Args:
        file_name (str): name of given file.
        output_dir (str): dir where file will be saved.
        output_dir (str): dir where file will be saved.
    Returns:
        Nothing.
    Prints:
        New file name and saved location.
    """
    file_path = input_dir + file_name
    output_file_name = file_name.split('.')[0] + '.parquet'
    output_file_path = output_dir + output_file_name
    pd.read_csv(file_path).to_parquet(output_file_path)
    print("File: '{0}' created at '{1}'".format(output_file_name, OUTPUT_DATA_DIR))


if __name__ == '__main__':
    for file in get_file_names():
        convert_csv_to_parquet(file)
