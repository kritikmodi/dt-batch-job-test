import datetime
import time
import os
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import pandas as pd
import gc

__version__ = '0.0.1'

def __batch_main__(sub_job_name, scheduled_time, runtime, part_num, num_parts, job_config, rundate, *args):
    num_rows = 10**7
    num_columns = 10
    file_path = '/tmp/test_parquet.parquet'
    print('Creating large Parquet file...')
    data = {f'col_{i}': np.random.rand(num_rows) for i in range(num_columns)}
    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, file_path)
    print(f'Parquet file created at : {file_path}')
    duration = 7200
    interval = 120
    time.sleep(interval)
    num_iterations = duration // interval
    print('Starting memory operations...\n\n\n')
    for _ in range(num_iterations):
        try:
            parquet_file = pd.read_parquet(file_path)
            print(f'Created parquet file reference')
            del parquet_file
            gc.collect()
            print(f'Deleted parquet file reference\n')
            time.sleep(interval)
        except Exception as e:
            print(f'Error occurred: {e}')

if __name__ == '__main__':
    __batch_main__(None, datetime.datetime.now(), None, None, None, {'A': 123, "test": True}, datetime.datetime.now().date(), ['a'], ['b'], ['c'])
