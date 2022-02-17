import logging
from operator import ge
import os
import json
import re
import sys
import base64
from typing import OrderedDict
from absl import app
from absl import flags
from download_utils import download_url_list_iterations

module_dir_ = os.path.dirname(__file__)
sys.path.append(os.path.join(module_dir_, '..'))
from common_utils.requests_wrappers import request_url_json


FLAGS = flags.FLAGS

flags.DEFINE_string('table_id', None,
                    'Table ID of the subject table to be downloaded e.g.S0101')
flags.DEFINE_integer('start_year', 2010,
                    'Start year of the data to be downloaded')
flags.DEFINE_integer('end_year', 2019,
                    'End year of the data to be downloaded')
flags.DEFINE_string('geo_map', './geo_url_map.json',
                    'Path of JSON file containing the list of summary levels to be downloaded')
flags.DEFINE_string('output_path', None,
                    'The folder where downloaded data is to be stored. Each table will have a sub directory created within this folder')
flags.DEFINE_string('api_key', None,
                    'API key sourced from census via https://api.census.gov/data/key_signup.html')

def get_url_variables(year, variables_str, geo_str):
    return f"https://api.census.gov/data/{year}/acs/acs5/subject?for={geo_str}&get={variables_str}"
def get_url_table(year, table_id, geo_str):
    return f"https://api.census.gov/data/{year}/acs/acs5/subject?get=group({table_id})&for={geo_str}"
def url_add_api_key(url_dict: dict, api_key: str) -> str:
    return (url_dict['url']+f'&key={api_key}').replace(' ', '%20')
def save_resp_json(resp, store_path):
    resp_data = resp.json()
    logging.info('Writing downloaded data to file: %s', store_path)
    json.dump(resp_data, open(store_path, 'w'), indent = 2)

def goestr_to_file_name(geo_str):
    geo_str = geo_str.replace(':*', '')
    geo_str = geo_str.replace('%20', '-')
    geo_str = geo_str.replace(' ', '-')
    geo_str = geo_str.replace('/', '')
    # geo_str = geo_str.replace('&in=state:', '_state_')
    geo_str = geo_str.replace('&in=', '_')
    geo_str = geo_str.replace(':', '')
    return geo_str

def get_file_name_table(output_path, table_id, year, geo_str):
    table_id = table_id.upper()
    output_path = os.path.expanduser(output_path)
    output_path = os.path.abspath(output_path)
    # TODO use base64 for long filename
    file_name = os.path.join(output_path, table_id+'_'+str(year)+'_'+goestr_to_file_name(geo_str)+'.csv')
    return file_name

def get_file_name_variables(output_path, table_id, year, chunk_id, geoStr):
    table_id = table_id.upper()
    output_path = os.path.expanduser(output_path)
    output_path = os.path.abspath(output_path)
    file_name = os.path.join(output_path, table_id+'_'+str(year)+'_'+goestr_to_file_name(geoStr)+'_'+str(chunk_id)+'.csv')
    return file_name

def find_summary_level(s_level_dict, geo_str):
    for s_level in s_level_dict:
        if s_level_dict[s_level]['str'] == geo_str:
            return s_level
    return ''

def is_required_hierarchical(req_list, geo_config_year):
    last_geo = req_list[-1]
    last_geo_id  = find_summary_level(geo_config_year['summary_levels'], last_geo)
    return (req_list[:-1] == geo_config_year['summary_levels'][last_geo_id]['geo_filters'])
    # NOTE: alternative implimentation could be(would need changes to compile_hierarchy_geo_str_list):
    # is_hierarchy = True
    # for cur_req in req_list[:-1]:
    #     if cur_req not in geo_config_year['summary_levels'][last_geo_id]['geo_filters']:
    #         is_hierarchy = False
    # return is_hierarchy

def compile_hierarchy_req_str_list(geo_list: dict, str_list: list) -> list:
    ret_list = []
    for k, v in geo_list.items():
        if isinstance(v, dict):
            new_list = str_list.copy()
            new_list[1] = f'{str_list[0]}{k}{str_list[1]}'
            ret_list.extend(compile_hierarchy_req_str_list(v, new_list[1:]))
        else:
            ret_list.append(f'{str_list[0]}{k}')
    return ret_list

# NOTE: code assumes that all fields appear in sequence and dependent geo levels are already present if list
def get_str_list_required(geo_config_year: dir, s_level: str):
    req_list = geo_config_year['summary_levels'][s_level]['requires'].copy()
    if len(req_list) > 0:
        str_list = []
        str_list.append('&in=' + req_list[0] + ':')
        if len(req_list) > 1:
            for s in req_list[1:]:
                str_list.append(' ' + s + ':')
        # print(s_level)
        # print(req_list)
        is_hierarchy = is_required_hierarchical(req_list, geo_config_year)
        if is_hierarchy:
            req_str_list = compile_hierarchy_req_str_list(geo_config_year['required_geo_lists'][req_list[-1]], str_list)
            # print(geo_str_list)
        else:
            req_str_list = []
    else:
        req_str_list = ['']
    
    return req_str_list

def update_geo_list(json_resp, geo_config, year, geo_str, s_level):
    print(geo_str)
    if geo_str not in geo_config[year]['required_geo_lists']:
        geo_config[year]['required_geo_lists'][geo_str] = {}
    
    name_i = json_resp[0].index('NAME')
    geo_i = json_resp[0].index(geo_str)
    filter_i = []
    
    for filter in geo_config[year]['summary_levels'][s_level]['geo_filters']:
        filter_i.append(json_resp[0].index(filter))
    for t in json_resp[1:]:
        d = geo_config[year]['required_geo_lists'][geo_str]
        for req_i in filter_i:
            if t[req_i] not in d:
                d[t[req_i]] = {}
            d = d[t[req_i]]
        d[t[geo_i]] = t[name_i]

def get_config_temp_filename(year, geo_str, req_str):
    s = f"{year}__{geo_str}__{req_str}"
    s = base64.b64encode(s.encode()).decode("utf-8")
    return f"{s}.json"

def get_yearwise_required_geos(geo_config: dict, api_key: str = '', force_fetch=True) -> dict:
    output_path = './tmp'
    for year in geo_config:
        print(year)
        if 'required_geo_lists' not in geo_config[year]:
            geo_config[year]['required_geo_lists'] = {}
        for geo_str in geo_config[year]['required_geos']:
            s_level = find_summary_level(geo_config[year]['summary_levels'], geo_str)
            if s_level:
                if force_fetch or geo_str not in geo_config[year]['required_geo_lists']:
                    req_str_list = get_str_list_required(geo_config[year], s_level)
                    url_list = []
                    for req_str in req_str_list:
                        temp_dict = {}
                        temp_dict['url'] = f"https://api.census.gov/data/{year}/acs/acs5/subject?get=NAME,S0101_C01_001E&for={geo_str}:*{req_str}"
                        temp_dict['store_path'] = os.path.join(output_path, get_config_temp_filename(year, geo_str, req_str))
                        temp_dict['status'] = 'pending'
                        url_list.append(temp_dict)
                    failed_ctr = download_url_list_iterations(url_list, url_add_api_key, api_key, save_resp_json, output_path)
                    if failed_ctr > 0:
                        download_url_list_iterations(url_list, url_add_api_key, api_key, save_resp_json, output_path)
                    for cur_url in url_list:
                        dir, filename = os.path.split(cur_url['store_path'])
                        s = base64.b64decode(filename.encode()).decode("utf-8")
                        arg = s.split('__')
                        temp = json.load(open(cur_url['store_path']))
                        update_geo_list(temp, geo_config, arg[0], arg[1], s_level)
            else:
                print('Warning:', geo_str, 'not found')
    return geo_config

def get_geographies(year_list, api_key: str = '', force_fetch=True) -> dict:
    basic_cache_path = os.path.join('.', 'geo_config', 'yearwise_config_basic.json')
    cache_path = os.path.join('.', 'geo_config', 'yearwise_config.json')
    # improve cache method, year list might change
    if os.path.isfile(cache_path):
        geo_config = json.load(open(cache_path, 'r'))
    elif os.path.isfile(basic_cache_path):
        geo_config = json.load(open(basic_cache_path, 'r'))
    else:
        geo_config = {}
        # geo_config['hierarchy'] = {}
    for year in year_list:
        if force_fetch or year not in geo_config:
            temp = request_url_json(f'https://api.census.gov/data/{year}/acs/acs5/subject/geography.json')
            geo_config[year] = OrderedDict()
            geo_config[year]['required_geos'] = []
            geo_config[year]['summary_levels'] = OrderedDict()
            for s_level in temp['fips']:
                geo_config[year]['summary_levels'][s_level['geoLevelDisplay']] = {}
                geo_config[year]['summary_levels'][s_level['geoLevelDisplay']]['str'] = s_level['name']
                if 'requires' in s_level:
                    geo_config[year]['summary_levels'][s_level['geoLevelDisplay']]['geo_filters'] = s_level['requires']
                else:
                    geo_config[year]['summary_levels'][s_level['geoLevelDisplay']]['geo_filters'] = []
                if 'wildcard' in s_level:
                    geo_config[year]['summary_levels'][s_level['geoLevelDisplay']]['wildcard'] = s_level['wildcard']
                else:
                    geo_config[year]['summary_levels'][s_level['geoLevelDisplay']]['wildcard'] = []
                geo_config[year]['summary_levels'][s_level['geoLevelDisplay']]['requires'] = []
                for geo in geo_config[year]['summary_levels'][s_level['geoLevelDisplay']]['geo_filters']:
                    if geo not in geo_config[year]['summary_levels'][s_level['geoLevelDisplay']]['wildcard']:
                        geo_config[year]['summary_levels'][s_level['geoLevelDisplay']]['requires'].append(geo)
                        if geo not in geo_config[year]['required_geos']:
                            geo_config[year]['required_geos'].append(geo)
    
    os.makedirs(os.path.dirname(basic_cache_path), exist_ok=True)
    with open(basic_cache_path, 'w') as fp:
        json.dump(geo_config, fp, indent=2)
    
    geo_config = get_yearwise_required_geos(geo_config, api_key)
    with open(cache_path, 'w') as fp:
        json.dump(geo_config, fp, indent=2)
    return geo_config

def get_yearwise_variable_column_map(table_id, year_list, store_path = None, force_fetch = True):
    if not store_path:
        store_path = f'table_variables_map/{table_id.upper()}_variable_column_map.json'
    if not force_fetch and os.path.isfile(store_path):
        temp_dict = json.load(open(store_path, 'r'))
        ret_dict = {}
        for year in temp_dict:
            ret_dict[int(year)] = temp_dict[year]
    else:
        table_id = table_id.upper()
        ret_dict = {}
        for year in year_list:
            temp = request_url_json(f"https://api.census.gov/data/{year}/acs/acs5/subject/groups/{table_id}.json")
            if temp:    
                ret_dict[year] = {}
                for var in temp['variables']:
                    ret_dict[year][var] = temp['variables'][var]['label']
                with open(store_path, 'w') as fp:
                    json.dump(ret_dict, fp, indent=2)
    return ret_dict

def get_url_entry_table(year, table_id, geo_str, output_path):
    tempDict = {}
    tempDict['url'] = get_url_table(year, table_id, geo_str)
    tempDict['store_path'] = get_file_name_table(output_path, table_id, year, geo_str)
    tempDict['status'] = 'pending'
    return tempDict

def get_table_url_list(table_id, year_list, output_path, api_key, s_level_list = 'all'):
    table_id = table_id.upper()
    ret_list = []
    geo_config = get_geographies(year_list, api_key)
    # get list of all s levels
    if s_level_list == 'all':
        s_level_list = []
        for year, year_dict in geo_config.items():
            for s_level in year_dict['summary_levels']:
                if s_level not in s_level_list:
                    s_level_list.append(s_level)
    
    for year in year_list:
        for s_level in s_level_list:
            if s_level in geo_config[year]['summary_levels']:
                req_str_list = get_str_list_required(geo_config[year], s_level)
                s_dict = geo_config[year]['summary_levels'][s_level]
                if req_str_list:
                    for geo_req in req_str_list:
                        ret_list.append(get_url_entry_table(year, table_id, f"{s_dict['str']}:*{geo_req}", output_path))
                else:
                    ret_list.append(get_url_entry_table(year, table_id, f"{s_dict['str']}:*", output_path))
            else:
                print('Warning:', s_level, 'not available for year', year)
    return ret_list

def get_yearwise_column_variable_map(table_id, year_list, store_path = None, force_fetch = True):
    if not store_path:
        store_path = f'table_variables_map/{table_id.upper()}_column_variable_map.json'
    if not force_fetch and os.path.isfile(store_path):
        temp_dict = json.load(open(store_path, 'r'))
        ret_dict = {}
        for year in temp_dict:
            ret_dict[int(year)] = temp_dict[year]
    else:
        table_id = table_id.upper()
        ret_dict = {}
        for year in year_list:
            temp = request_url_json(f"https://api.census.gov/data/{year}/acs/acs5/subject/groups/{table_id}.json")
            if temp:    
                ret_dict[year] = {}
                for var in temp['variables']:
                    ret_dict[year][temp['variables'][var]['label']] = var
                with open(store_path, 'w') as fp:
                    json.dump(ret_dict, fp, indent=2)
    return ret_dict

def get_variables_url_list(table_id, variables_year_dict, geo_url_map, output_path, api_key):
    table_id = table_id.upper()
    ret_list = []
    
    year_list = []
    for year in variables_year_dict:
        year_list.append(year)

    states_by_year = get_yearwise_state_list(year_list, 'state_list.json', api_key)
    for year in variables_year_dict:
        # limited to 50 variables including NAME
        n = 49
        variables_chunked = [variables_year_dict[year][i:i + n] for i in range(0, len(variables_year_dict[year]), n)]
        logging.info('variable list divided into %d chunks for %d', len(variables_chunked), year)
        for geo_id in geo_url_map:
            geo_str = geo_url_map[geo_id]['urlStr']
            if geo_url_map[geo_id]['needsStateID']:
                for state_id in states_by_year[year]:
                    geo_str_state = geo_str + state_id
                    for i, curVars in enumerate(variables_chunked):
                        variable_list_str = ','.join(curVars)
                        temp_dict = {}
                        temp_dict['url'] = get_url_variables(year, 'NAME,' + variable_list_str, geo_str_state)
                        temp_dict['store_path'] = get_file_name_variables(output_path, table_id, year, i, geo_str_state)
                        temp_dict['status'] = 'pending'
                        ret_list.append(temp_dict)
            else:
                for i, curVars in enumerate(variables_chunked):
                    variable_list_str = ','.join(curVars)
                    temp_dict = {}
                    temp_dict['url'] = get_url_variables(year, 'NAME,' + variable_list_str, geo_str)
                    temp_dict['store_path'] = get_file_name_variables(output_path, table_id, year, i, geo_str)
                    temp_dict['status'] = 'pending'
                    ret_list.append(temp_dict)
    return ret_list

def main(argv):
    geo_url_map = json.load(open(FLAGS.geo_map))
    year_list = list(range(FLAGS.start_year, FLAGS.end_year+1))
    out_path = os.path.expanduser(FLAGS.output_path)
    url_list = get_table_url_list(FLAGS.table_id, year_list, out_path, FLAGS.api_key)
    os.makedirs(os.path.join(out_path, FLAGS.table_id), exist_ok=True)
    with open(os.path.join(out_path, FLAGS.table_id, 'download_status.json'), 'w') as fp:
        json.dump(url_list, fp, indent=2)

if __name__ == '__main__':
  flags.mark_flags_as_required(['table_id', 'output_path', 'api_key'])
  app.run(main)