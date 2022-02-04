import logging
import os
import json
from download_utils import request_url_json

def get_url_variables(year, variablesStr, geoStr, apiKey):
    return f"https://api.census.gov/data/{year}/acs/acs5/subject?for={geoStr}&key={apiKey}&get={variablesStr}"
def get_url_table(year, tableID, geoStr, apiKey):
    return f"https://api.census.gov/data/{year}/acs/acs5/subject?get=group({tableID})&for={geoStr}&key={apiKey}"

def goestr_to_file_name(geo_str):
    geo_str = geo_str.replace(':*', '')
    geo_str = geo_str.replace('%20', '-')
    geo_str = geo_str.replace('/', '')
    geo_str = geo_str.replace('&in=state:', '_state_')
    return geo_str

def get_file_name_table(output_path, table_id, year, geo_str):
    table_id = table_id.upper()
    file_name = output_path+table_id+'_'+str(year)+'_'+goestr_to_file_name(geo_str)+'.csv'
    return file_name

def get_file_name_variables(output_path, table_id, year, chunk_id, geoStr):
    table_id = table_id.upper()
    file_name = output_path+table_id+'_'+str(year)+'_'+goestr_to_file_name(geoStr)+'_'+str(chunk_id)+'.csv'
    return file_name

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
    retList = []
    states_by_year = get_yearwise_state_list(year_list, 'state_list.json')
    for year in year_list:
        for geoID in geo_url_map:
            geo_str = geo_url_map[geoID]['urlStr']
            if geo_url_map[geoID]['needsStateID']:
                for state_id in states_by_year[year]:
                    geo_str_state = geo_str + state_id
                    tempDict = get_url_entry_table(year, table_id, geo_str_state, output_path, api_key)
                    retList.append(tempDict)
            else:
                tempDict = get_url_entry_table(year, table_id, geo_str, output_path, api_key)
                retList.append(tempDict)
    return retList

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

def get_variables_url_list(table_id, variables_year_dict, geoURLMap, output_path, api_key):
    table_id = table_id.upper()
    ret_list = []
    
    year_list = []
    for year in variables_year_dict:
        year_list.append(year)

    states_by_year = get_yearwise_state_list(year_list)
    for year in variables_year_dict:
        # limited to 50 variables including NAME
        n = 49
        variablesChunked = [variables_year_dict[year][i:i + n] for i in range(0, len(variables_year_dict[year]), n)]
        logging.info('variable list divided into %d chunks for %d', len(variablesChunked), year)
        for geo_id in geoURLMap:
            geoStr = geoURLMap[geo_id]['urlStr']
            if geoURLMap[geo_id]['needsStateID']:
                for state_id in states_by_year[year]:
                    geoStrState = geoStr + state_id
                    for i, curVars in enumerate(variablesChunked):
                        variableListStr = ','.join(curVars)
                        tempDict = {}
                        tempDict['url'] = get_url_variables(year, 'NAME,' + variableListStr, geoStrState, api_key)
                        tempDict['store_path'] = get_file_name_variables(output_path, table_id, year, i, geoStrState)
                        tempDict['status'] = 'pending'
                        ret_list.append(tempDict)
            else:
                for i, curVars in enumerate(variablesChunked):
                    variableListStr = ','.join(curVars)
                    tempDict = {}
                    tempDict['url'] = get_url_variables(year, 'NAME,' + variableListStr, geoStr, api_key)
                    tempDict['store_path'] = get_file_name_variables(output_path, table_id, year, i, geoStr)
                    tempDict['status'] = 'pending'
                    ret_list.append(tempDict)
    return ret_list