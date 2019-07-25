import os
import shutil
import pyarrow as pa
import pyarrow.parquet as pq


from csv_to_parquet.main.main import get_file_names, convert_csv_to_parquet

INPUT_DATA_DIR = f'{os.getcwd()}/test/test_data/'
OUTPUT_DATA_DIR = f'{os.getcwd()}/test/test_data/output_data/'

parquet_file = pq.ParquetFile(INPUT_DATA_DIR + 'test.parquet')
expected_content = parquet_file.scan_contents()
expected_schema = parquet_file.schema


def test_get_file_names():
    assert get_file_names(path= INPUT_DATA_DIR) == ['test_file.csv']


def test_convert_csv_to_parquet():
    # Prepare output folder
    shutil.rmtree(OUTPUT_DATA_DIR, ignore_errors=True)
    os.mkdir(OUTPUT_DATA_DIR)

    # Converted file to parquet
    convert_csv_to_parquet(file_name='test_file.csv', input_dir=INPUT_DATA_DIR, output_dir=OUTPUT_DATA_DIR)

    # Read converted file
    result_parquet = pq.ParquetFile(OUTPUT_DATA_DIR + 'test_file.parquet')

    # Compare with expected results
    assert expected_content == result_parquet.scan_contents()
    assert expected_schema == result_parquet.schema
    
    # Remove temp data
    shutil.rmtree(OUTPUT_DATA_DIR, ignore_errors=True)

