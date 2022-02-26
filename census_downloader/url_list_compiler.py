import logging
from operator import ge
import os
import json
import sys
import base64
from typing import Any, Union
from absl import app
from absl import flags

from status_file_utils import sync_status_list
from census_api_helpers import *

module_dir_ = os.path.dirname(__file__)
sys.path.append(os.path.join(module_dir_, '..'))

FLAGS = flags.FLAGS

flags.DEFINE_integer('start_year', 2010,
                    'Start year of the data to be downloaded')
flags.DEFINE_integer('end_year', 2019,
                    'End year of the data to be downloaded')
flags.DEFINE_list('summary_levels', None,
                    'List of summary levels to be downloaded e.g. 040, 060')
flags.DEFINE_string('output_path', None,
                    'The folder where downloaded data is to be stored. Each dataset and table will have a sub directory created within this folder')
flags.DEFINE_boolean('all_summaries', None,
                     'Download data for all available summary levels')

def get_url_variables(dataset: str, year: str, variables_str: str, geo_str: str) -> str:
    return f"https://api.census.gov/data/{year}/{dataset}?for={geo_str}&get={variables_str}"
def get_url_table(dataset: str, year: str, table_id: str, geo_str: str) -> str:
    return f"https://api.census.gov/data/{year}/{dataset}?get=group({table_id})&for={geo_str}"

def save_resp_json(resp: Any, store_path: str):
    resp_data = resp.json()
    logging.info('Writing downloaded data to file: %s', store_path)
    json.dump(resp_data, open(store_path, 'w'), indent = 2)

def goestr_to_file_name(geo_str: str) -> str:
    geo_str = geo_str.replace(':*', '')
    geo_str = geo_str.replace('%20', '-')
    geo_str = geo_str.replace(' ', '-')
    geo_str = geo_str.replace('/', '')
    # geo_str = geo_str.replace('&in=state:', '_state_')
    geo_str = geo_str.replace('&in=', '_')
    geo_str = geo_str.replace(':', '')
    return geo_str

def get_file_name_table(output_path: str, table_id: str, year: str, geo_str: str) -> str:
    table_id = table_id.upper()
    output_path = os.path.expanduser(output_path)
    output_path = os.path.abspath(output_path)
    # TODO use base64 for long filename
    file_name = os.path.join(output_path, table_id+'_'+str(year)+'_'+goestr_to_file_name(geo_str)+'.csv')
    return file_name

def get_file_name_variables(output_path: str, table_id: str, year: str, chunk_id: int, geoStr: str) -> str:
    table_id = table_id.upper()
    output_path = os.path.expanduser(output_path)
    output_path = os.path.abspath(output_path)
    file_name = os.path.join(output_path, table_id+'_'+str(year)+'_'+goestr_to_file_name(geoStr)+'_'+str(chunk_id)+'.csv')
    return file_name

def get_url_entry_table(dataset: str, year: str, table_id: str, geo_str: str, output_path: str, force_fetch: bool = False) -> dict:
    temp_dict = {}
    temp_dict['url'] = get_url_table(dataset, year, table_id, geo_str)
    temp_dict['store_path'] = get_file_name_table(output_path, table_id, year, geo_str)
    temp_dict['status'] = 'pending'
    if force_fetch:
        temp_dict['force_fetch'] = True
    return temp_dict

def get_table_url_list(dataset: str, table_id: str, q_variable: str, year_list: list, output_path: str, api_key: str, s_level_list: Union[list, str] = 'all', force_fetch_config: bool = False, force_fetch_data: bool = False) -> list:
    table_id = table_id.upper()
    if dataset not in get_list_datasets(force_fetch=force_fetch_config):
        print(dataset, 'not found')
        return []
    if table_id not in get_dataset_groups(dataset, force_fetch=force_fetch_config):
        print(table_id, 'not found in ', dataset)
        return []
    available_years = get_dataset_groups_years(dataset, table_id, force_fetch=force_fetch_config)
    geo_config = get_summary_level_config(dataset, q_variable, api_key, force_fetch_config)
    # get list of all s levels
    if s_level_list == 'all':
        s_level_list = []
        for year, year_dict in geo_config.items():
            for s_level in year_dict['summary_levels']:
                if s_level not in s_level_list:
                    s_level_list.append(s_level)
    ret_list = []
    for year in year_list:
        if year in available_years:
            print('urls ', year)
            for s_level in s_level_list:
                if s_level in geo_config[year]['summary_levels']:
                    req_str_list = get_str_list_required(geo_config[year], s_level)
                    s_dict = geo_config[year]['summary_levels'][s_level]
                    if req_str_list:
                        for geo_req in req_str_list:
                            ret_list.append(get_url_entry_table(dataset, year, table_id, f"{s_dict['str']}:*{geo_req}", output_path, force_fetch_data))
                    else:
                        ret_list.append(get_url_entry_table(dataset, year, table_id, f"{s_dict['str']}:*", output_path, force_fetch_data))
                else:
                    print('Warning:', s_level, 'not available for year', year)
    ret_list = sync_status_list([], ret_list)
    return ret_list

def get_variables_url_list(dataset: str, table_id, q_variable, variables_year_dict, output_path, api_key, s_level_list = 'all', force_fetch_config = False, force_fetch_data = False):
    pass
    # table_id = table_id.upper()
    # ret_list = []
    
    # geo_config = get_summary_level_config(dataset, q_variable, api_key, force_fetch_config)
    # # get list of all s levels
    # if s_level_list == 'all':
    #     s_level_list = []
    #     for year, year_dict in geo_config.items():
    #         for s_level in year_dict['summary_levels']:
    #             if s_level not in s_level_list:
    #                 s_level_list.append(s_level)
    
    # year_list = []
    # for year in variables_year_dict:
    #     year_list.append(year)
    
    # for year in variables_year_dict:
    #     # limited to 50 variables including NAME
    #     n = 49
    #     variables_chunked = [variables_year_dict[year][i:i + n] for i in range(0, len(variables_year_dict[year]), n)]
    #     logging.info('variable list divided into %d chunks for year %d', len(variables_chunked), year)
        # for geo_id in geo_url_map:
        #     geo_str = geo_url_map[geo_id]['urlStr']
        #     if geo_url_map[geo_id]['needsStateID']:
        #         for state_id in states_by_year[year]:
        #             geo_str_state = geo_str + state_id
        #             for i, cur_vars in enumerate(variables_chunked):
        #                 variable_list_str = ','.join(cur_vars)
        #                 temp_dict = {}
        #                 temp_dict['url'] = get_url_variables(dataset, year, 'NAME,' + variable_list_str, geo_str_state)
        #                 temp_dict['store_path'] = get_file_name_variables(output_path, table_id, year, i, geo_str_state)
        #                 temp_dict['status'] = 'pending'
        #                 ret_list.append(temp_dict)
        #     else:
        #         for i, cur_vars in enumerate(variables_chunked):
        #             variable_list_str = ','.join(cur_vars)
        #             temp_dict = {}
        #             temp_dict['url'] = get_url_variables(dataset, year, 'NAME,' + variable_list_str, geo_str)
        #             temp_dict['store_path'] = get_file_name_variables(output_path, table_id, year, i, geo_str)
        #             temp_dict['status'] = 'pending'
        #             ret_list.append(temp_dict)
    return ret_list

def main(argv):
    # geo_url_map = json.load(open(FLAGS.geo_map))
    year_list_int = list(range(FLAGS.start_year, FLAGS.end_year+1))
    year_list = [str(y) for y in year_list_int]
    out_path = os.path.expanduser(FLAGS.output_path)
    if FLAGS.summary_levels:
        s_list = FLAGS.summary_levels
    else:
        s_list = 'all'
    url_list = get_table_url_list(FLAGS.dataset, FLAGS.table_id, FLAGS.q_variable, year_list, out_path, FLAGS.api_key, s_level_list=s_list, force_fetch=FLAGS.force_fetch_config)
    os.makedirs(os.path.join(out_path, FLAGS.table_id), exist_ok=True)
    print('Writing URLs to file')
    with open(os.path.join(out_path, FLAGS.table_id, 'download_status.json'), 'w') as fp:
        json.dump(url_list, fp, indent=2)

if __name__ == '__main__':
  flags.mark_flags_as_required(['table_id', 'output_path', 'api_key'])
  flags.mark_flags_as_mutual_exclusive(['summary_levels', 'all_summaries'], required=True)
  app.run(main)