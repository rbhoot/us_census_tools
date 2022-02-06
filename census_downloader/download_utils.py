import json
import logging
import os
import random
import time
import pandas as pd
from status_file_utils import read_update_status, get_pending_or_fail_url_list, get_pending_url_list
import grequests

def create_delay(t):
    time.sleep(t + (random.random() / 2 ))

def download_url_list_iterations(url_list, url_api_modifier, api_key, output_path, max_itr = 10, retry_failed = True):
    failed_urls_ctr = len(url_list)
    prev_failed_ctr = failed_urls_ctr + 1
    loop_ctr = 0
    logging.info('downloading URLs')
    while failed_urls_ctr > 0 and loop_ctr < max_itr and prev_failed_ctr > failed_urls_ctr:
        prev_failed_ctr = failed_urls_ctr
        logging.info('downloading URLs iteration:%d', loop_ctr)
        failed_urls_ctr = download_url_list(url_list, url_api_modifier, api_key, output_path, loop_ctr, retry_failed)
        logging.info('failed request count: %d', failed_urls_ctr)
        loop_ctr += 1
    return failed_urls_ctr

def download_url_list(url_list, url_api_modifier, api_key, output_path, ctr, retry_failed = True):
    # logging.debug('Downloading url list %s', ','.join(url_list))
    logging.debug('Output path: %s, Iteration: %d', output_path, ctr)
    
    status_path = os.path.join(output_path, 'download_status.json')
    url_list_all = read_update_status(status_path, url_list)
    if retry_failed:
        url_list = get_pending_or_fail_url_list(url_list_all)
    else:
        url_list = get_pending_url_list(url_list_all)
    
    # keep this as the number of parallel requests targeted
    n = 30
    urls_chunked = [url_list[i:i + n] for i in range(0, len(url_list), n)]
    fail_ctr = 0

    print("Downloading", len(url_list), "urls in chunks of", n, ", iteration:", ctr)

    logging.info("%d URLs to be downloaded for iteration %d", len(url_list), ctr)
    if ctr > 3:
        create_delay(35)
        logging.info('Creating 35 sec delay because of > 3 iterations')
    for j, cur_chunk in enumerate(urls_chunked):
        start_t = time.time()
        results = grequests.map((grequests.get(url_api_modifier(u, api_key)) for u in cur_chunk), size=n)
        delay_flag = False
        for i, resp in enumerate(results):
            if resp:
                # NOTE: use commented line when debug of url with api key is needed 
                # logging.info('%s response code %d', resp.url, resp.status_code)
                logging.info('%s response code %d', url_list[j*n+i]['url'], resp.status_code)
                if resp.status_code == 200:
                    resp_data = resp.json()
                    headers = resp_data.pop(0)
                    df = pd.DataFrame(resp_data, columns=headers)
                    # print(cur_chunk[i]['store_path'])
                    logging.info('Writing downloaded data to file: %s', cur_chunk[i]['store_path'])
                    df.to_csv(cur_chunk[i]['store_path'], encoding='utf-8', index = False)
                    url_list[j*n+i]['status'] = 'ok'
                    url_list[j*n+i]['http_code'] = str(resp.status_code)
                else:
                    url_list[j*n+i]['status'] = 'fail'
                    url_list[j*n+i]['http_code'] = str(resp.status_code)
                    print("HTTP status code: "+str(resp.status_code))
            else:
                delay_flag = True
                print("Error: None reponse obj", cur_chunk[i]['url'])
                logging.warn('%s resonsed None', cur_chunk[i]['url'])
                url_list[j*n+i]['status'] = 'fail'
                fail_ctr += 1
        end_t = time.time()
        logging.debug('Storing download status')
        with open(status_path, 'w') as fp:
            json.dump(url_list_all, fp, indent=2)
        
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