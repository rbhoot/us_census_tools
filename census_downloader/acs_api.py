import zipfile
import csv
import json
import io
import random
import time
import os
import pandas as pd
import grequests
import requests
import datetime
import logging

'''
TODO
    command line arguments
        hide api key

    2010 download county subdivision, zip tabulation

'''

def create_delay(t):
    time.sleep(t + (random.random() / 2 ))

def get_url_variables(year, variablesStr, geoStr, apiKey):
    return f"https://api.census.gov/data/{year}/acs/acs5/subject?for={geoStr}&key={apiKey}&get={variablesStr}"
def get_url_table(year, tableID, geoStr, apiKey):
    return f"https://api.census.gov/data/{year}/acs/acs5/subject?get=group({tableID})&for={geoStr}&key={apiKey}"

def exception(request, exception):
        print("Problem: {}: {}".format(request.url, exception))

def goeStr_to_file_name(geoStr):
    geoStr = geoStr.replace(':*', '')
    geoStr = geoStr.replace('%20', '-')
    geoStr = geoStr.replace('/', '')
    geoStr = geoStr.replace('&in=state:', '_state_')
    return geoStr

def download_url_list(url_list, output_path, ctr):
    # logging.debug('Downloading url list %s', ','.join(url_list))
    logging.debug('Output path: %s, Iteration: %d', output_path, ctr)
    
    if os.path.isfile(output_path+'download_status.json'):
        logging.debug('Found previous download status file')
        status_list = json.load(open(output_path+'download_status.json', 'r'))
        for url_status in status_list:
            if url_status['status'] != 'fail':
                for url_temp in url_list:
                    if url_temp['url'] == url_status['url']:
                        url_list.remove(url_temp)
                if url_status['status'] != 'downloaded' and url_status['status'] != '204':
                    logging.info('%s url responded with %s HTTP code', url_status['url'], url_list['status'])
    else:
        logging.debug('No previous download status file')
        status_list = []
    # keep this as the number of parallel requests targeted
    fail_ctr = 0
    n = 30
    urls_chunked = [url_list[i:i + n] for i in range(0, len(url_list), n)]
    
    print("Downloading", len(url_list), "urls in chunks of", n, ", iteration:", ctr)

    logging.info("%d URLs to be downloaded for iteration %d", len(url_list), ctr)
    if ctr > 3:
        create_delay(35)
        logging.info('Creating 35 sec delay because of > 3 iterations')
    for cur_chunk in urls_chunked:
        start_t = time.time()
        # logging.debug('Initializing parallel request fot url list %s', ','.join(url_list))
        results = grequests.map((grequests.get(u['url']) for u in cur_chunk), size=n)
        delay_flag = False
        for i, resp in enumerate(results):
            if resp:
                logging.info('%s response code %d', resp.url, resp.status_code)
                # print(resp.url)
                if resp.status_code == 200:
                    resp_data = resp.json()
                    headers = resp_data.pop(0)
                    df = pd.DataFrame(resp_data, columns=headers)
                    # print(cur_chunk[i]['name'])
                    logging.info('Writing downloaded data to file: %s', cur_chunk[i]['name'])
                    df.to_csv(cur_chunk[i]['name'], encoding='utf-8', index = False)
                    status_list[-1]['status'] = 'downloaded'
                else:
                    status_list[-1]['status'] = str(resp.status_code)
                    print("HTTP status code: "+str(resp.status_code))
            else:
                delay_flag = True
                print("Error: None reponse obj", cur_chunk[i]['url'])
                logging.warn('%s resonsed None', cur_chunk[i]['url'])
                status_list[-1]['status'] = 'fail'
                fail_ctr += 1

            status_list.append(cur_chunk[i])
        end_t = time.time()
        logging.debug('Storing download status')
        with open(output_path+'download_status.json', 'w') as fp:
            json.dump(status_list, fp, indent=2)
        
        print("The time required to download", n, "URLs :", end_t-start_t)
        if delay_flag:
            logging.info('Creating 20 sec delay')
            create_delay(20)
        if ctr > 1 and ctr < 3:
            logging.info('Creating %d sec delay', 5+3*ctr)
            create_delay(5+3*ctr)
        else:
            logging.info('Creating 8 sec delay')
            create_delay(8)
    return fail_ctr

def get_file_name_table(output_path, table_id, year, geoStr):
    table_id = table_id.upper()
    file_name = output_path+table_id+'_'+str(year)+'_'+goeStr_to_file_name(geoStr)+'.csv'
    return file_name

def get_table_url_list(table_id, year_list, geo_url_map, output_path, api_key):
    table_id = table_id.upper()
    retList = []
    states_by_year = get_yearwise_state_list(year_list, 'state_list.json')
    for year in year_list:
        for geoID in geo_url_map:
            geoStr = geo_url_map[geoID]['urlStr']
            if geo_url_map[geoID]['needsStateID']:
                for stateID in states_by_year[year]:
                    geoStrState = geoStr + stateID
                    tempDict = {}
                    tempDict['url'] = get_url_table(year, table_id, geoStrState, api_key)
                    tempDict['name'] = get_file_name_table(output_path, table_id, year, geoStrState)
                    tempDict['status'] = 'fail'
                    retList.append(tempDict)
            else:
                tempDict = {}
                tempDict['url'] = get_url_table(year, table_id, geoStr, api_key)
                tempDict['name'] = get_file_name_table(output_path, table_id, year, geoStr)
                tempDict['status'] = 'fail'
                retList.append(tempDict)
    return retList

def download_table(table_id, year_list, geoURLMapPath, output_path, api_key):
    logging.info('Downloading table:%s to %s', table_id, output_path)
    table_id = table_id.upper()
    geoURLMap = json.load(open(geoURLMapPath, 'r'))
    logging.debug('creating missing directories in path:%s', output_path)
    if not os.path.exists(output_path):
        os.makedirs(output_path, exist_ok=True)
    
    logging.info('compiling list of URLs')
    url_list = get_table_url_list(table_id, year_list, geoURLMap, output_path, api_key)
    
    status_file = output_path+'download_status.json'
    if not os.path.isfile(status_file):
        logging.debug('Storing initial download status')
        with open(status_file, 'w') as fp:
            json.dump(url_list, fp, indent=2)

    print(len(url_list))
    logging.info("Compiled a list of %d URLs", len(url_list))

    start = time.time()

    failed_urls_ctr = len(url_list)
    loop_ctr = 0
    logging.info('downloading URLs')
    prev_failed_ctr = failed_urls_ctr + 1
    while failed_urls_ctr > 0 and loop_ctr < 10 and prev_failed_ctr > failed_urls_ctr:
        prev_failed_ctr = failed_urls_ctr
        logging.info('downloading URLs iteration:%d', loop_ctr)
        failed_urls_ctr = download_url_list(url_list, output_path, loop_ctr)
        logging.info('failed request count: %d', failed_urls_ctr)
        loop_ctr += 1

    consolidate_files(table_id, year_list, output_path)
    
    end = time.time()
    print("The time required to download the", table_id, "dataset :", end-start)
    logging.info('The time required to download the %s dataset : %f', table_id, end-start)

def get_file_name_variables(output_path, table_id, year, chunk_id, geoStr):
    table_id = table_id.upper()
    file_name = output_path+table_id+'_'+str(year)+'_'+goeStr_to_file_name(geoStr)+'_'+str(chunk_id)+'.csv'
    return file_name

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
                        tempDict['name'] = get_file_name_variables(output_path, table_id, year, i, geoStrState)
                        tempDict['status'] = 'fail'
                        ret_list.append(tempDict)
            else:
                for i, curVars in enumerate(variablesChunked):
                    variableListStr = ','.join(curVars)
                    tempDict = {}
                    tempDict['url'] = get_url_variables(year, 'NAME,' + variableListStr, geoStr, api_key)
                    tempDict['name'] = get_file_name_variables(output_path, table_id, year, i, geoStr)
                    tempDict['status'] = 'fail'
                    ret_list.append(tempDict)
    return ret_list

def download_table_variables(table_id, year_list, geoURLMapPath, spec_path, output_path, api_key):
    table_id = table_id.upper()
    spec_dict = json.load(open(spec_path, 'r'))
    geoURLMap = json.load(open(geoURLMapPath, 'r'))
    
    if not os.path.exists(output_path):
        os.makedirs(output_path, exist_ok=True)
    
    variables_year_dict = {}
    variable_col_map = get_yearwise_variable_column_map(table_id, year_list, 'table_variables_map/'+table_id+'_variable_column_map.json')
    print(list(variable_col_map))
    for year in year_list:
        variables_year_dict[year] = []
        for variable_id in variable_col_map[year]:
            column_name = variable_col_map[year][variable_id]
            t_flag = True
            if 'ignoreColumns' in spec_dict:
                for ig_col in spec_dict['ignoreColumns']:
                    if '!!' in ig_col:
                        if ig_col in column_name:
                            t_flag = False
                    if ig_col in column_name.split('!!'):
                        t_flag = False
            if t_flag:            
                variables_year_dict[year].append(variable_id)
        print(year)
        print(len(variables_year_dict[year]))
                        
    url_list = get_variables_url_list(table_id, variables_year_dict, geoURLMap, output_path, api_key)

    status_file = output_path+'download_status.json'
    if not os.path.isfile(status_file):
        logging.debug('Storing initial download status')
        with open(status_file, 'w') as fp:
            json.dump(url_list, fp, indent=2)

    print(len(url_list))
    logging.info("Compiled a list of %d URLs", len(url_list))

    start = time.time()

    failed_urls_ctr = len(url_list)
    loop_ctr = 0
    prev_failed_ctr = failed_urls_ctr + 1
    while failed_urls_ctr > 0 and loop_ctr < 10 and prev_failed_ctr > failed_urls_ctr:
        prev_failed_ctr = failed_urls_ctr
        failed_urls_ctr = download_url_list(url_list, output_path, loop_ctr)
        logging.info('failed request count: %d', failed_urls_ctr)
        loop_ctr += 1

    # consolidate_files(table_id, year_list, output_path)

    end = time.time()
    print("The time required to download the", table_id, "dataset :", end-start)
    logging.info('The time required to download the %s dataset : %f', table_id, end-start)

def request_url_json(url):
    req = requests.get(url)
    print(req.url)
    if req.status_code == requests.codes.ok:
        response_data = req.json()
        #print(response_data)
    else:
        response_data = {}
        print("HTTP status code: "+str(req.status_code))
        #if req.status_code != 204:
            #TODO
    return response_data

def get_yearwise_state_list(year_list, store_path = None, api_key=''):
    try:
        temp_dict = json.load(open('state_list.json', 'r'))
        ret_dict = {}
        for year in temp_dict:
            ret_dict[int(year)] = temp_dict[year]
    except IOError:
        ret_dict = {}
        for year in year_list:
            temp = request_url_json(f"https://api.census.gov/data/{year}/acs/acs5/subject?get=NAME,S0101_C01_001E&for=state:*&key={api_key}")
            if temp:
                ret_dict[year] = {}
                for row in temp:
                    if 'NAME' not in row:
                        ret_dict[year][row[2]] = row[0]
                if store_path:
                    with open(store_path, 'w') as fp:
                        json.dump(ret_dict, fp, indent=2)
    return ret_dict

def get_yearwise_column_variable_map(table_id, year_list, store_path = None):
    try:
        temp_dict = json.load(open(f'table_variables_map/{table_id.upper()}_column_variable_map.json', 'r'))
        ret_dict = {}
        for year in temp_dict:
            ret_dict[int(year)] = temp_dict[year]
    except IOError:
        table_id = table_id.upper()

        ret_dict = {}
        for year in year_list:
            temp = request_url_json(f"https://api.census.gov/data/{year}/acs/acs5/subject/groups/{table_id}.json")
            if temp:    
                ret_dict[year] = {}
                for var in temp['variables']:
                    ret_dict[year][temp['variables'][var]['label']] = var
        if store_path:
            with open(store_path, 'w') as fp:
                json.dump(ret_dict, fp, indent=2)
    return ret_dict

def get_yearwise_variable_column_map(table_id, year_list, store_path = None):
    try:
        temp_dict = json.load(open(f'table_variables_map/{table_id.upper()}_variable_column_map.json', 'r'))
        ret_dict = {}
        for year in temp_dict:
            ret_dict[int(year)] = temp_dict[year]
    except IOError:
        table_id = table_id.upper()

        ret_dict = {}
        for year in year_list:
            temp = request_url_json(f"https://api.census.gov/data/{year}/acs/acs5/subject/groups/{table_id}.json")
            if temp:    
                ret_dict[year] = {}
                for var in temp['variables']:
                    ret_dict[year][var] = temp['variables'][var]['label']
        if store_path:
            with open(store_path, 'w') as fp:
                json.dump(ret_dict, fp, indent=2)
    return ret_dict

def consolidate_files(table_id, year_list, output_path, keep_originals=True):
    logging.info('consolidating files to create yearwise files in %s', output_path)
    logging.info('table:%s keep_originals:%d', table_id, keep_originals)
    table_id = table_id.upper()

    csv_files_list = {}
    out_csv_list = []

    for (dirpath, dirnames, filenames) in os.walk(output_path):
        for file in filenames:
            if file.endswith('.csv'):
                for year in year_list:
                    if str(year) in file and 'ACS' not in file:
                        if year in csv_files_list:
                            csv_files_list[year].append(file)
                        else:
                            csv_files_list[year] = [file]
    
    total_files = 0
    for year in csv_files_list:
        total_files += len(csv_files_list[year])

    print(len(csv_files_list), total_files)    
    logging.info('consolidating %d files', total_files)
    for year in csv_files_list:
        print(year)
        logging.info('consolidating %d files for year:%d', len(csv_files_list[year]), year)
        df = pd.DataFrame()
        for csv_file in csv_files_list[year]:
            df2 = pd.read_csv(output_path+csv_file,low_memory=False)
            print("Collecting",csv_file)
            # remove extra columns
            drop_list = []
            for column_name in list(df2):
                # substitute annotations
                if table_id in column_name and 'A' != column_name[-1]:
                    df2.loc[df2[column_name+'A'].notna(), column_name] = df2[column_name+'A']
                    drop_list.append(column_name+'A')
                if column_name not in ['GEO_ID', 'NAME'] and table_id not in column_name:
                    if column_name not in drop_list:
                        drop_list.append(column_name)
                        logging.debug('dropping %s column in file:%s', column_name, output_path+csv_file)
            df2.drop(drop_list, axis=1, inplace=True)
            
            # if df2.lt(-100).any().any():
            #     print("Error: Check", output_path+csv_file, "annotation not replaced in some column")
            # if df2.lt(0).any().any():
            #     print("Warning: Check", output_path+csv_file, "file contains negative values")
            if 'GEO_ID' not in list(df2) or 'NAME' not in list(df2):
                print("Error: Check", output_path+csv_file, "GEO_ID or NAME column missing")
                logging.error('GEO_ID or NAME column missing in file:%s', output_path+csv_file)
            if df2['GEO_ID'].isnull().any():
                print("Error: Check", output_path+csv_file, "GEO_ID column missing has missing data")
                logging.error('GEO_ID missing data in file:%s', output_path+csv_file)

            if df.empty:
                var_col_lookup = get_yearwise_variable_column_map(table_id, year_list, 'table_variables_map/'+table_id+'_variable_column_map.json')
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
            out_file_name = f"{output_path}ACSST5Y{year}.{table_id}_data_with_overlays_1111-11-11T111111.csv"
            
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
    with zipfile.ZipFile(output_path+table_id+'.zip', 'w') as zipMe:        
        for file in out_csv_list:
            zipMe.write(file, arcname=file.replace(output_path,''), compress_type=zipfile.ZIP_DEFLATED)
    
    if not keep_originals:
        print("Deleting old files")
        logging.info('deleting seperated files')
        for year in csv_files_list:
            print("Deleting", len(csv_files_list[year]), "files of year", year)
            logging.info('deleting %d files of year %d', len(csv_files_list[year]), year)
            for csv_file in csv_files_list[year]:
                print("Deleting", output_path+csv_file)
                logging.info('deleting %s', output_path+csv_file)
                if os.path.isfile(output_path+csv_file):
                     os.remove(output_path+csv_file)
        # logging.info('deleting download status file')
        # status_file = output_path+'download_status.json'
        # print("Deleting", status_file)
        # if os.path.isfile(status_file):
        #     os.remove(status_file)





logging.basicConfig(filename=f"logs/acs_download_{datetime.datetime.now().replace(microsecond=0).isoformat().replace(':','')}.log", level=logging.DEBUG, format="%(asctime)s [%(levelname)s]: %(message)s")

# start = time.time()
# consolidate_files('S1810', list(range(2013, 2020)), 'data3/')
# get_yearwise_column_variable_map('s1810', list(range(2013, 2020)), 'table_variables_map/S1810_column_variable_map.json')
# get_yearwise_variable_column_map('s1810', list(range(2013, 2020)), 'table_variables_map/S1810_variable_column_map.json')
# get_yearwise_variable_column_map('S0503', list(range(2011, 2020)), 'table_variables_map/S0503_variable_column_map.json')
# get_yearwise_state_list(list(range(2010, 2020)), 'state_list.json')

# download_table('S1810', list(range(2013, 2020)), 'geoURLMap.json', 's1810/', api_key)
# download_table('S1702', list(range(2010, 2020)), 'geoURLMap.json', 's1702/', api_key)
# download_table('S2702', list(range(2013, 2020)), 'geoURLMap.json', 's2702/', api_key)
# download_table_variables('S1810', list(range(2013, 2020)), 'geoURLMap.json', 'test_spec.json', 's1810_var/', api_key)
# end = time.time()
# print("The time required to download the entire dataset :", end-start)


# def getVariableMapping():
