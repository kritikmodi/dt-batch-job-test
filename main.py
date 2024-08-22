import datetime
import logging
import time
from pandas import DataFrame
import pyarrow.parquet as pq

__version__ = '0.0.1'

def __batch_main__(sub_job_name, scheduled_time, runtime, part_num, num_parts, job_config, rundate, *args):
    print(f'{job_config=}, type={type(job_config)}')
    print(f'{scheduled_time=}, type={type(scheduled_time)}')
    print(f'{sub_job_name=}')
    print(f'{rundate=}')
    print('-'  * 120)
    print('Some logging:')
    logging.debug('this is debug')
    logging.info('This is info')
    logging.warning('this is warning')
    logging.error('this is error')
    logging.critical('this is critical')
    print('-'  * 120)
    for i, arg in enumerate(args):
        print(f'{i=}, {arg=}')
    
    file_path = '/mnt/nfs/home/mkazants/data/2023.parquet'
    duration = 3600
    interval = 10
    num_iterations = duration // interval
    
    for _ in range(num_iterations):
        try:
            parquet_file = pq.ParquetFile(file_path)
            print(f'Successfully opened {file_path}')
            parquet_file = None
            print(f'Closed {file_path}')
        except Exception as e:
            print(f'Failed to open {file_path}: {e}')
        time.sleep(interval)
    
    result = {sub_job_name: [1, 2, 3]}
    return result

if __name__ == '__main__':
    __batch_main__(None, datetime.datetime.now(), None, None, None, {'A': 123, "test": True}, datetime.datetime.now().date(), ['a'], ['b'], ['c'])
