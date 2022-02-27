import asyncio
import zipfile
import json
import time
import os
import sys
import pandas as pd
import logging
import datetime
from typing import Any, Callable, Union
from absl import app
from absl import flags

from census_api_helpers import get_identifier, get_yearwise_variable_column_map
from status_file_utils import sync_status_list
from download_utils import download_url_list_iterations
from url_list_compiler import get_table_url_list

module_dir_ = os.path.dirname(__file__)
sys.path.append(os.path.join(module_dir_, '..'))
from common_utils.requests_wrappers import request_url_json

FLAGS = flags.FLAGS

flags.DEFINE_boolean('force_fetch_data', False,
                     'Force download of all data from API')
def url_add_api_key(url_dict: dict, api_key: str) -> str:
    return url_dict['url']+f'&key={api_key}'

def save_resp_csv(resp_data: list, store_path: str) -> int:
    headers = resp_data.pop(0)
    df = pd.DataFrame(resp_data, columns=headers)
    logging.info('Writing downloaded data to file: %s', store_path)
    df.to_csv(store_path, encoding='utf-8', index = False)
    return 0

async def async_save_resp_csv(resp: Any, store_path: str) -> int:
    try:
        resp_data = await resp.json()
    except asyncio.TimeoutError:
        print('Error: Response parsing timing out.')
        return -1
    headers = resp_data.pop(0)
    df = pd.DataFrame(resp_data, columns=headers)
    logging.info('Writing downloaded data to file: %s', store_path)
    df.to_csv(store_path, encoding='utf-8', index = False)
    return 0

async def update_status_periodically(interval: int, periodic_function: Callable[[], None]):
    while True:
        await asyncio.gather(asyncio.sleep(interval))
        periodic_function()

def log_to_status(url_list: list, store_path: str):
    with open(store_path, 'w') as fp:
        json.dump(url_list, fp, indent=2)

def url_filter(url_list: list) -> list:
    ret_list = []
    for cur_url in url_list:
        if cur_url['status'] == 'pending' or cur_url['status'].startswith('fail'):
            if 'http_code' in cur_url: 
                if cur_url['http_code'] != '204':
                    ret_list.append(cur_url)
            else:
                ret_list.append(cur_url)
    return ret_list

def download_table(dataset: str, table_id: str, q_variable: str, year_list: list, output_path: str, api_key: str, s_level_list: Union[str, list] = 'all', force_fetch_config: bool = False, force_fetch_data: bool = False):
    logging.info('Downloading table:%s to %s', table_id, output_path)
    table_id = table_id.upper()
    output_path = os.path.expanduser(output_path)
    output_path = os.path.join(output_path, dataset)
    output_path = os.path.join(output_path, table_id)
    logging.debug('creating missing directories in path:%s', output_path)
    os.makedirs(output_path, exist_ok=True)
    
    logging.info('compiling list of URLs')
    url_list = get_table_url_list(dataset, table_id, q_variable, year_list, output_path, api_key, s_level_list, force_fetch_config, force_fetch_data)
    
    status_path = os.path.join(output_path, 'download_status.json')
    
    if os.path.isfile(status_path):
        log_list = json.load(open(status_path))
    else:
        log_list = []
    url_list = sync_status_list(log_list, url_list)
    with open(status_path, 'w') as fp:
        json.dump(url_list, fp, indent=2)
    print(len(url_list))
    logging.info("Compiled a list of %d URLs", len(url_list))

    start = time.time()

    rate_params = {}
    rate_params['max_parallel_req'] = 50
    rate_params['limit_per_host'] = 20
    rate_params['req_per_unit_time'] = 10
    rate_params['unit_time'] = 1

    failed_urls_ctr = download_url_list_iterations(url_list, url_add_api_key, api_key, async_save_resp_csv, url_filter=url_filter, rate_params=rate_params)
    # TODO log at regular interval
    # asyncio.run(update_status_periodically(15, log_to_status(url_list, status_path)))

    with open(status_path, 'w') as fp:
        json.dump(url_list, fp, indent=2)
    
    # check status before consolidate, warn if any URL status contains fail
    if failed_urls_ctr > 0:
        logging.warning('%d urls have failed, trying with requests.', failed_urls_ctr)
        cur_url_list = url_filter(url_list)
        for cur_url in cur_url_list:
            resp = request_url_json(url_add_api_key(cur_url, api_key))
            if 'http_err_code' in resp:
                cur_url['status'] = 'fail_http'
                cur_url['http_code'] = str(resp['http_err_code'])
            else:
                save_resp_csv(resp, cur_url['store_path'])
                cur_url['status'] = 'ok'
                cur_url['http_code'] = '200'
        
        with open(status_path, 'w') as fp:
            json.dump(url_list, fp, indent=2)
    
    failed_urls_ctr = len(url_filter(url_list))
    if failed_urls_ctr > 0:
        logging.warning('%d urls have failed, output files might be missing data.', failed_urls_ctr)

    consolidate_files(dataset, table_id, year_list, output_path)
    
    end = time.time()
    print("The time required to download the", table_id, "dataset :", end-start)
    logging.info('The time required to download the %s dataset : %f', table_id, end-start)

# TODO make the function faster by parallel processing for each year
def consolidate_files(dataset: str, table_id: str, year_list: list, output_path: str, replace_annotations: bool = True, drop_annotations: bool = True, keep_originals: bool = True):
    logging.info('consolidating files to create yearwise files in %s', output_path)
    logging.info('table:%s keep_originals:%d', table_id, keep_originals)
    table_id = table_id.upper()

    csv_files_list = {}
    out_csv_list = []

    identifier_dict = {}
    for year in year_list:
        identifier_dict[year] = get_identifier(dataset, year)

    for (dirpath, dirnames, filenames) in os.walk(output_path):
        for file in filenames:
            if file.endswith('.csv'):
                for year in year_list:
                    identifier = identifier_dict[year]
                    if '_' in file and identifier not in file:
                        file_tokens = file.split('_')
                        file_year = file_tokens[1]
                        if file_year == str(year):
                            if year in csv_files_list:
                                csv_files_list[year].append(file)
                            else:
                                csv_files_list[year] = [file]
    
    total_files = 0
    for year in csv_files_list:
        total_files += len(csv_files_list[year])

    print(len(csv_files_list), total_files)    
    logging.info('consolidating %d files', total_files)
    var_col_lookup = get_yearwise_variable_column_map(dataset, table_id, list(csv_files_list))
    for year in csv_files_list:
        print(year)
        # TODO error handling when identifier is missing
        identifier = identifier_dict[year]
        logging.info('consolidating %d files for year:%s', len(csv_files_list[year]), year)
        df = pd.DataFrame()
        for csv_file in csv_files_list[year]:
            cur_csv_path = os.path.join(output_path, csv_file)
            df2 = pd.read_csv(cur_csv_path,low_memory=False)
            print("Collecting",csv_file)
            # remove extra columns
            drop_list = []
            for column_name in list(df2):
                # substitute annotations
                if table_id in column_name and column_name[-1] != 'A':
                    if replace_annotations:
                        df2.loc[df2[column_name+'A'].notna(), column_name] = df2[column_name+'A']
                    if drop_annotations:
                        drop_list.append(column_name+'A')
                if column_name not in ['GEO_ID', 'NAME'] and table_id not in column_name:
                    if column_name not in drop_list:
                        drop_list.append(column_name)
                        logging.debug('dropping %s column in file:%s', column_name, cur_csv_path)
            df2.drop(drop_list, axis=1, inplace=True)
            
            # if df2.lt(-100).any().any():
            #     print("Error: Check", output_path+csv_file, "annotation not replaced in some column")
            # if df2.lt(0).any().any():
            #     print("Warning: Check", output_path+csv_file, "file contains negative values")
            if 'GEO_ID' not in list(df2) or 'NAME' not in list(df2):
                print("Error: Check", cur_csv_path, "GEO_ID or NAME column missing")
                logging.error('GEO_ID or NAME column missing in file:%s', cur_csv_path)
            if df2['GEO_ID'].isnull().any():
                print("Error: Check", cur_csv_path, "GEO_ID column missing has missing data")
                logging.error('GEO_ID missing data in file:%s', cur_csv_path)

            if df.empty:
                new_row = []
                for column_name in list(df2):
                    if column_name == 'GEO_ID':
                        new_row.append('id')
                    elif column_name == 'NAME':
                        new_row.append('Geographic Area Name')
                    elif table_id in column_name:
                        new_row.append(var_col_lookup[year][column_name])
                    else:
                        new_row.append('')
                logging.info('appending column names to dataframe')
                df2.loc[-1] = new_row
                df2.index = df2.index + 1
                df2.sort_index(inplace=True)
            df = pd.concat([df, df2], ignore_index = True)
        if not df.empty:
            out_file_name = os.path.join(output_path, f"{identifier}.{table_id}_data_with_overlays_1111-11-11T111111.csv")
            if df.iloc[0].isnull().any():
                print("Error: Check", out_file_name, "column name missing for some variable")
                logging.error('some column names missing in:%s', out_file_name)
            if df['GEO_ID'].isnull().any():
                print("Error: Check", out_file_name, "GEO_ID column missing has missing data")
                logging.error('GEO_ID column data missing in:%s', out_file_name)
            logging.info('writing combined data to:%s', out_file_name)
            df.to_csv(out_file_name, encoding='utf-8', index=False)
            out_csv_list.append(out_file_name)

    print("zipppin")
    print(out_csv_list)
    logging.info('zipping output files')
    with zipfile.ZipFile(os.path.join(output_path, table_id+'.zip'), 'w') as zipMe:        
        for file in out_csv_list:
            zipMe.write(file, arcname=file.replace(output_path,''), compress_type=zipfile.ZIP_DEFLATED)
    
    if not keep_originals:
        print("Deleting old files")
        logging.info('deleting seperated files')
        for year in csv_files_list:
            print("Deleting", len(csv_files_list[year]), "files of year", year)
            logging.info('deleting %d files of year %d', len(csv_files_list[year]), year)
            for csv_file in csv_files_list[year]:
                cur_csv_path = os.path.join(output_path, csv_file)
                print("Deleting", cur_csv_path)
                logging.info('deleting %s', cur_csv_path)
                if os.path.isfile(cur_csv_path):
                     os.remove(cur_csv_path)

def download_table_variables(dataset, table_id, year_list, geo_url_map_path, spec_path, output_path, api_key):
    # TODO implement the method
    pass
#     table_id = table_id.upper()
#     spec_dict = json.load(open(spec_path, 'r'))
#     geo_url_map = json.load(open(geo_url_map_path, 'r'))
    
#     output_path = os.path.expanduser(output_path)
#     output_path = os.path.join(output_path, dataset)
#     output_path = os.path.join(output_path, table_id+'_vars')
#     os.makedirs(output_path, exist_ok=True)

#     status_path = os.path.join(output_path, 'download_status.json')
    
#     variables_year_dict = {}
#     variable_col_map = get_yearwise_variable_column_map(dataset, table_id, year_list)
#     print(list(variable_col_map))
#     for year in year_list:
#         variables_year_dict[year] = []
#         for variable_id in variable_col_map[year]:
#             column_name = variable_col_map[year][variable_id]
#             t_flag = True
#             if not column_to_be_ignored(column_name, spec_dict):
#                 variables_year_dict[year].append(variable_id)
#                 variables_year_dict[year].append(variable_id+'A')
#         print(year)
#         print(len(variables_year_dict[year]))
                        
#     url_list = get_variables_url_list(table_id, variables_year_dict, geo_url_map, output_path, api_key)
#     url_list = sync_status_list([], url_list)
#     with open(status_path, 'w') as fp:
#         json.dump(url_list, fp, indent=2)

#     print(len(url_list))
#     logging.info("Compiled a list of %d URLs", len(url_list))

#     start = time.time()

#     rate_params = {}
#     rate_params['max_parallel_req'] = 50
#     rate_params['limit_per_host'] = 20
#     rate_params['req_per_unit_time'] = 10
#     rate_params['unit_time'] = 1

#     failed_urls_ctr = download_url_list_iterations(url_list, url_add_api_key, api_key, async_save_resp_csv, url_filter=url_filter, rate_params=rate_params)

#     with open(status_path, 'w') as fp:
#         json.dump(url_list, fp, indent=2)

#     # check status before consolidate, warn if any URL status contains fail
#     if failed_urls_ctr > 0:
#         logging.warn('%d urls have failed, output files might be missing data.', failed_urls_ctr)
#     consolidate_files(dataset, table_id, year_list, output_path)

#     end = time.time()
#     print("The time required to download the", table_id, "dataset :", end-start)
#     logging.info('The time required to download the %s dataset : %f', table_id, end-start)

os.makedirs('logs/', exist_ok=True)
logging.basicConfig(filename=f"logs/acs_download_{datetime.datetime.now().replace(microsecond=0).isoformat().replace(':','')}.log", level=logging.DEBUG, format="%(asctime)s [%(levelname)s]: %(message)s")

def main(argv):
    year_list_int = list(range(FLAGS.start_year, FLAGS.end_year+1))
    year_list = [str(y) for y in year_list_int]
    out_path = os.path.expanduser(FLAGS.output_path)
    if FLAGS.summary_levels:
        s_list = FLAGS.summary_levels
    else:
        s_list = 'all'
    download_table(FLAGS.dataset, FLAGS.table_id, FLAGS.q_variable, year_list, out_path, FLAGS.api_key, s_list, FLAGS.force_fetch_config, FLAGS.force_fetch_data)
    
if __name__ == '__main__':
  flags.mark_flags_as_required(['table_id', 'output_path', 'api_key'])
  app.run(main)