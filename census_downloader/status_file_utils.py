import json
import os
from shutil import copy2
import base64

_VALID_STATUS = ['pending', 'ok', 'fail', 'fail_http']

def url_to_download(url_dict: dict):
    if url_dict['status'] == 'pending' or url_dict['status'].startswith('fail') or url_dict['force_fetch']:
        return True
    else:
        return False

# read status file, reconcile url list
def read_update_status(filename: str, url_list: list, force_fetch_all: bool = False) -> list:
    filename = os.path.expanduser(filename)
    
    if filename and os.path.isfile(filename):
        prev_status = json.load(open(filename))
    else:
        prev_status = []
    if force_fetch_all:
        for cur_url in url_list:
            cur_url['force_fetch'] = True
    final_list = sync_status_list(prev_status, url_list)
    
    # write back to the log file
    json.dump(final_list, open(filename, 'w'), indent=2)
    return final_list

# add urls or sync 2 url list
# def sync_status_list(log_list: list, new_list: list, store_path: str = '~/dc_data/') -> list:
def sync_status_list(log_list: list, new_list: list) -> list:
    ret_list = log_list.copy()
    for cur_url in new_list:
        if 'method' not in cur_url:
            cur_url['method'] = 'get'
        # if cur_url['method'].casefold() == 'get' and 'data' in cur_url:
        #     cur_url['url'] = url_add_data(cur_url['url'], cur_url['data'])
        elif 'data' not in cur_url:
            cur_url['data'] = None
       
        # store_path default value, expand user and abs
        if 'store_path' not in cur_url:
            # add file name
            # TODO make the filename work for post request which would have same URL with different data
            # cur_url['store_path'] = os.path.join(store_path, base64.b64encode(cur_url['url']))
            raise ValueError('Each url must have an associated store_path')
        cur_url['store_path'] = os.path.expanduser(cur_url['store_path'])
        cur_url['store_path'] = os.path.abspath(cur_url['store_path'])
        os.makedirs(os.path.dirname(cur_url['store_path']), exist_ok=True)
        
        # search in status
        url_found = False
        for i, log_url in enumerate(log_list):
            if not url_found:
                is_same = False
                # same url
                if cur_url['url'] == log_url['url']:
                    # same method
                    if cur_url['method'] == log_url['method']:
                        # same data
                        if 'data' in cur_url and 'data' in log_url:
                            if cur_url['data'] == log_url['data']:
                                is_same = True
                        # no data
                        # TODO check, handle case when data is None
                        elif cur_url['method'].casefold() == 'get':
                            is_same = True
                        elif cur_url['method'].casefold() != 'get' and 'data' not in cur_url and 'data' not in log_url:
                            is_same = True
                        
                if is_same:
                    url_found = True
                    # copy the related data
                    if 'http_code' in log_url:
                        cur_url['http_code'] = log_url['http_code']
                    if 'force_fetch' not in cur_url: 
                        cur_url['force_fetch'] = False
                    if cur_url['force_fetch']:
                        cur_url['status'] = 'pending'
                        cur_url.pop('http_code', None)
                    else:
                        # check file existence
                        if os.path.isfile(cur_url['store_path']):
                            cur_url['status'] = 'ok'
                        # copy file if store_path is different and status ok
                        elif os.path.isfile(log_url['store_path']) and log_url['status'] == 'ok':
                            copy2(log_url['store_path'], cur_url['store_path'])
                            cur_url['status'] = 'ok'
                        else:
                            cur_url['status'] = 'pending'
                            cur_url.pop('http_code', None)
                    ret_list[i] = cur_url
                    
        if not url_found:
            # force fetch
            if 'force_fetch' not in cur_url:
                cur_url['force_fetch'] = False
            if not cur_url['force_fetch'] and os.path.isfile(cur_url['store_path']):
                cur_url['status'] = 'ok'
            else:
                cur_url['status'] = 'pending'
            cur_url.pop('http_code', None)
            ret_list.append(cur_url)
        
        if 'status' not in cur_url:
            cur_url['status'] = 'pending'
        if cur_url['status'] not in _VALID_STATUS:
            print('Warning: Found invalid status for', cur_url['url'])
            cur_url['status'] = 'pending'
    return ret_list

# get to be downloaded urls
def get_pending_url_list(url_list: list) -> list:
    pending_url_list = []
    for cur_url in url_list:
        if cur_url['status'] == 'pending':
            pending_url_list.append(cur_url)
    return pending_url_list

def get_failed_url_list(url_list: list) -> list:
    pending_url_list = []
    for cur_url in url_list:
        if cur_url['status'].startswith('fail'):
            pending_url_list.append(cur_url)
    return pending_url_list

def get_failed_http_url_list(url_list: list) -> list:
    pending_url_list = []
    for cur_url in url_list:
        if cur_url['status'] == 'fail_http':
            pending_url_list.append(cur_url)
    return pending_url_list

def get_pending_or_fail_url_list(url_list: list) -> list:
    pending_url_list = []
    for cur_url in url_list:
        if cur_url['status'] == 'pending' or cur_url['status'].startswith('fail'):
            pending_url_list.append(cur_url)
    return pending_url_list
