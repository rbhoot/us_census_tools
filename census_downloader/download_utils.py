import json
import logging
import os
import random
import time
import requests
import pandas as pd
import grequests

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

def create_delay(t):
    time.sleep(t + (random.random() / 2 ))

def download_url_list(url_list, output_path, ctr):
    # logging.debug('Downloading url list %s', ','.join(url_list))
    logging.debug('Output path: %s, Iteration: %d', output_path, ctr)
    
    # TODO extract function status
    if os.path.isfile(os.path.join(output_path, 'download_status.json')):
        logging.debug('Found previous download status file')
        status_list = json.load(open(os.path.join(output_path, 'download_status.json'), 'r'))
        for url_status in status_list:
            if url_status['status'] != 'fail':
                for url_temp in url_list:
                    if url_temp['url'] == url_status['url']:
                        url_list.remove(url_temp)
                if url_status['status'] != 'ok' and url_status['status'] != '204':
                    logging.info('%s url responded with %s HTTP code', url_status['url'], url_list['status'])
    else:
        logging.debug('No previous download status file')
        status_list = []
    # keep this as the number of parallel requests targeted
    n = 30
    urls_chunked = [url_list[i:i + n] for i in range(0, len(url_list), n)]
    fail_ctr = 0

    print("Downloading", len(url_list), "urls in chunks of", n, ", iteration:", ctr)

    logging.info("%d URLs to be downloaded for iteration %d", len(url_list), ctr)
    if ctr > 3:
        create_delay(35)
        logging.info('Creating 35 sec delay because of > 3 iterations')
    for cur_chunk in urls_chunked:
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
                    # print(cur_chunk[i]['name'])
                    logging.info('Writing downloaded data to file: %s', cur_chunk[i]['name'])
                    df.to_csv(cur_chunk[i]['name'], encoding='utf-8', index = False)
                    # TODO extract function status
                    status_list[-1]['status'] = 'ok'
                else:
                    # TODO extract function status
                    status_list[-1]['status'] = str(resp.status_code)
                    print("HTTP status code: "+str(resp.status_code))
            else:
                delay_flag = True
                print("Error: None reponse obj", cur_chunk[i]['url'])
                logging.warn('%s resonsed None', cur_chunk[i]['url'])
                # TODO extract function status
                status_list[-1]['status'] = 'fail'
                fail_ctr += 1
            # TODO extract function status
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