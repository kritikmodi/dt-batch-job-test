import datetime
import time
import os
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import pandas as pd

__version__ = '0.0.1'

def __batch_main__(sub_job_name, scheduled_time, runtime, part_num, num_parts, job_config, rundate, *args):
    num_rows = 10**7
    num_columns = 10
    file_path = '/tmp/test_parquet.parquet'
    duration = 7200
    interval = 120
    num_iterations = duration // interval
    for _ in range(num_iterations):
        try:
            print('Creating large Parquet file...')
            data = {f'col_{i}': np.random.rand(num_rows) for i in range(num_columns)}
            df = pd.DataFrame(data)
            table = pa.Table.from_pandas(df)
            pq.write_table(table, file_path)
            print(f'Parquet file created at : {file_path}')
            parquet_file = pq.ParquetFile(file_path)
            print(f'Opened {file_path}')
            parquet_file = None
            print(f'Closed {file_path}')
            os.remove(file_path)
            print(f'Deleted {file_path}\n')
            time.sleep(interval)
        except Exception as e:
            print(f'Error occurred: {e}')

if __name__ == '__main__':
    __batch_main__(None, datetime.datetime.now(), None, None, None, {'A': 123, "test": True}, datetime.datetime.now().date(), ['a'], ['b'], ['c'])
