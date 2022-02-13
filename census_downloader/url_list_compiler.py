import logging
from operator import ge
import os
import json
import re
from typing import OrderedDict
from requests_wrappers import request_url_json
from absl import app
from absl import flags

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
# TODO attach api key while downloading and not before
flags.DEFINE_string('api_key', None,
                    'API key sourced from census via https://api.census.gov/data/key_signup.html')

def get_url_variables(year, variablesStr, geoStr, apiKey):
    return f"https://api.census.gov/data/{year}/acs/acs5/subject?for={geoStr}&get={variablesStr}"
def get_url_table(year, tableID, geoStr, apiKey):
    return f"https://api.census.gov/data/{year}/acs/acs5/subject?get=group({tableID})&for={geoStr}"

def goestr_to_file_name(geo_str):
    geo_str = geo_str.replace(':*', '')
    geo_str = geo_str.replace('%20', '-')
    geo_str = geo_str.replace('/', '')
    geo_str = geo_str.replace('&in=state:', '_state_')
    return geo_str

def get_file_name_table(output_path, table_id, year, geo_str):
    table_id = table_id.upper()
    output_path = os.path.expanduser(output_path)
    output_path = os.path.abspath(output_path)
    file_name = os.path.join(output_path, table_id+'_'+str(year)+'_'+goestr_to_file_name(geo_str)+'.csv')
    return file_name

def get_file_name_variables(output_path, table_id, year, chunk_id, geoStr):
    table_id = table_id.upper()
    output_path = os.path.expanduser(output_path)
    output_path = os.path.abspath(output_path)
    file_name = os.path.join(output_path, table_id+'_'+str(year)+'_'+goestr_to_file_name(geoStr)+'_'+str(chunk_id)+'.csv')
    return file_name

def get_geographies(year_list, api_key: str = '', force_fetch=True) -> dict:
    basic_cache_path = os.path.join('.', 'geo_config', 'yearwise_config_basic.json')
    cache_path = os.path.join('.', 'geo_config', 'yearwise_config.json')
    if not force_fetch and os.path.isfile(cache_path):
        geo_config = json.load(open(cache_path, 'r'))
    else:
        geo_config = {}
        # geo_config['hierarchy'] = {}
        for year in year_list:
            temp = request_url_json(f'https://api.census.gov/data/{year}/acs/acs5/subject/geography.json')
            geo_config[year] = OrderedDict()
            geo_config[year]['required_geos'] = []
            geo_config[year]['summary_levels'] = {}
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

def get_yearwise_required_geos(geo_config: dict, api_key: str = '') -> dict:
    for year in geo_config:
        print(year)
        geo_config[year]['required_geo_lists'] = {}
        url_list = []
        for geo_str in geo_config[year]['required_geos']:
            found_f = False
            for s_level in geo_config[year]['summary_levels']:
                s_level_dict = geo_config[year]['summary_levels'][s_level]
                if not found_f and s_level_dict['str'] == geo_str:
                    found_f = True
                    url_dict = {}
                    if s_level_dict['requires']:
                        # NOTE: code assumes that all fields appear in sequence and dependent geo levels are already present if list
                        # TODO check if all previous s_level_dict['requires'] in s_level_dict['requires'][-1]
                        # TODO recursive function call to compile list of URLs for s_level_dict['requires'][-1]
                        # for req_geo_str in s_level_dict['requires']:
                        #     for geo_id in geo_config[year]['required_geo_lists'][req_geo_str]:
                                
                        url_dict['url'] = f"https://api.census.gov/data/{year}/acs/acs5/subject?get=NAME,S0101_C01_001E&for={geo_str}:*&in={req_geo_str}:{geo_id}&key={api_key}"
                        if len(s_level_dict['requires']) == 1 and s_level_dict['requires'][0] in geo_config[year]['required_geo_lists']:
                            req_geo_str = s_level_dict['requires'][0]
                            for geo_id in geo_config[year]['required_geo_lists'][req_geo_str]:
                                temp = request_url_json(f"https://api.census.gov/data/{year}/acs/acs5/subject?get=NAME,S0101_C01_001E&for={geo_str}:*&in={req_geo_str}:{geo_id}&key={api_key}")
                                geo_config[year]['required_geo_lists'][geo_str] = {}
                                name_i = temp[0].index('NAME')
                                geo_i = temp[0].index(geo_str)
                                req_i = temp[0].index(req_geo_str)
                                for t in temp[1:]:
                                    if t[req_i] not in geo_config[year]['required_geo_lists'][geo_str]:
                                        geo_config[year]['required_geo_lists'][geo_str][t[req_i]] = {}
                                    geo_config[year]['required_geo_lists'][geo_str][t[req_i]][t[geo_i]] = t[name_i]
                        else:
                            print(geo_str, s_level_dict['requires'])
                    else:
                        temp = request_url_json(f"https://api.census.gov/data/{year}/acs/acs5/subject?get=NAME,S0101_C01_001E&for={geo_str}:*&key={api_key}")
                        geo_config[year]['required_geo_lists'][geo_str] = {}
                        name_i = temp[0].index('NAME')
                        geo_i = temp[0].index(geo_str)
                        for t in temp[1:]:
                            geo_config[year]['required_geo_lists'][geo_str][t[geo_i]] = t[name_i]
    return geo_config




def get_yearwise_state_list(year_list, store_path = 'state_list.json', api_key='', force_fetch = True):
    if not force_fetch and os.path.isfile(store_path):
        temp_dict = json.load(open('state_list.json', 'r'))
        ret_dict = {}
        for year in temp_dict:
            ret_dict[int(year)] = temp_dict[year]
    else:
        ret_dict = {}
        for year in year_list:
            temp = request_url_json(f"https://api.census.gov/data/{year}/acs/acs5/subject?get=NAME,S0101_C01_001E&for=state:*&key={api_key}")
            if temp:
                ret_dict[year] = {}
                for row in temp:
                    if 'NAME' not in row:
                        ret_dict[year][row[2]] = row[0]
                with open(store_path, 'w') as fp:
                    json.dump(ret_dict, fp, indent=2)
    return ret_dict

def get_yearwise_county_list(year_list, store_path = 'county_list.json', api_key='', force_fetch = True):
    if not force_fetch and os.path.isfile(store_path):
        temp_dict = json.load(open('county_list.json', 'r'))
        ret_dict = {}
        for year in temp_dict:
            ret_dict[int(year)] = temp_dict[year]
    else:
        ret_dict = {}
        for year in year_list:
            temp = request_url_json(f"https://api.census.gov/data/{year}/acs/acs5/subject?get=NAME,S0101_C01_001E&for=county:*&key={api_key}")
            if temp:
                ret_dict[year] = {}
                for row in temp:
                    if 'NAME' not in row:
                        if row[2] not in ret_dict[year]:
                            ret_dict[year][row[2]] = {}
                        ret_dict[year][row[2]][row[3]] = row[0]
                with open(store_path, 'w') as fp:
                    json.dump(ret_dict, fp, indent=2)
    return ret_dict

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

def get_url_entry_table(year, table_id, geo_str, output_path, api_key):
    tempDict = {}
    tempDict['url'] = get_url_table(year, table_id, geo_str, api_key)
    tempDict['store_path'] = get_file_name_table(output_path, table_id, year, geo_str)
    tempDict['status'] = 'pending'
    return tempDict

def get_table_url_list(table_id, year_list, geo_url_map, output_path, api_key):
    table_id = table_id.upper()
    ret_list = []
    get_geographies(year_list, api_key)
    # states_by_year = get_yearwise_state_list(year_list, 'state_list.json', api_key, False)
    # county_by_year = get_yearwise_county_list(year_list, 'county_list.json', api_key, False)
    for year in year_list:
        for geo_id in geo_url_map:
            geo_str = geo_url_map[geo_id]['urlStr']
            if geo_url_map[geo_id]['needsStateID']:
                for state_id in states_by_year[year]:
                    geo_str_state = geo_str + state_id
                    temp_dict = get_url_entry_table(year, table_id, geo_str_state, output_path, api_key)
                    ret_list.append(temp_dict)
            else:
                temp_dict = get_url_entry_table(year, table_id, geo_str, output_path, api_key)
                ret_list.append(temp_dict)
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
                        temp_dict['url'] = get_url_variables(year, 'NAME,' + variable_list_str, geo_str_state, api_key)
                        temp_dict['store_path'] = get_file_name_variables(output_path, table_id, year, i, geo_str_state)
                        temp_dict['status'] = 'pending'
                        ret_list.append(temp_dict)
            else:
                for i, curVars in enumerate(variables_chunked):
                    variable_list_str = ','.join(curVars)
                    temp_dict = {}
                    temp_dict['url'] = get_url_variables(year, 'NAME,' + variable_list_str, geo_str, api_key)
                    temp_dict['store_path'] = get_file_name_variables(output_path, table_id, year, i, geo_str)
                    temp_dict['status'] = 'pending'
                    ret_list.append(temp_dict)
    return ret_list

def main(argv):
    geo_url_map = json.load(open(FLAGS.geo_map))
    year_list = list(range(FLAGS.start_year, FLAGS.end_year+1))
    out_path = os.path.expanduser(FLAGS.output_path)
    url_list = get_table_url_list(FLAGS.table_id, year_list, geo_url_map, out_path, FLAGS.api_key)
    os.makedirs(os.path.join(out_path, FLAGS.table_id), exist_ok=True)
    with open(os.path.join(out_path, FLAGS.table_id, 'download_status.json'), 'w') as fp:
        json.dump(url_list, fp, indent=2)

if __name__ == '__main__':
  flags.mark_flags_as_required(['table_id', 'output_path', 'api_key'])
  app.run(main)