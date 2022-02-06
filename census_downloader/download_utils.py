import json
import logging
import os
import random
import time
import pandas as pd
from status_file_utils import read_update_status, get_pending_or_fail_url_list
import grequests

def create_delay(t):
    time.sleep(t + (random.random() / 2 ))

def download_url_list(url_list, output_path, ctr):
    # logging.debug('Downloading url list %s', ','.join(url_list))
    logging.debug('Output path: %s, Iteration: %d', output_path, ctr)
    
    status_path = os.path.join(output_path, 'download_status.json')
    url_list_all = read_update_status(status_path, url_list)
    url_list = get_pending_or_fail_url_list(url_list_all)
    
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
        # logging.debug('Initializing parallel request for url list %s', ','.join(url_list))
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
            # # TODO extract function status
            # status_list.append(cur_chunk[i])
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