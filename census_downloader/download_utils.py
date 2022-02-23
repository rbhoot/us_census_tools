import json
import logging
import time
import asyncio
import aiohttp
from aiolimiter import AsyncLimiter

from status_file_utils import get_pending_or_fail_url_list, url_to_download

async def async_save_resp_json(resp, store_path):
    resp_data = await resp.json()
    logging.debug('Writing downloaded data to file: %s', store_path)
    json.dump(resp_data, open(store_path, 'w'), indent = 2)

def download_url_list_iterations(url_list, url_api_modifier, api_key, process_and_store, max_itr = 10, rate_params = {}):
    failed_urls_ctr = len(url_list)
    prev_failed_ctr = failed_urls_ctr + 1
    loop_ctr = 0
    logging.info('downloading URLs')
    while failed_urls_ctr > 0 and loop_ctr < max_itr and prev_failed_ctr > failed_urls_ctr:
        prev_failed_ctr = failed_urls_ctr
        logging.info('downloading URLs iteration:%d', loop_ctr)
        failed_urls_ctr = download_url_list(url_list, url_api_modifier, api_key, process_and_store, rate_params)
        logging.info('failed request count: %d', failed_urls_ctr)
        loop_ctr += 1
    return failed_urls_ctr

# req url
async def fetch(session, cur_url, semaphore, limiter, url_api_modifier, api_key, process_and_store):
# async def fetch(session, url, semaphore):
    if url_to_download(cur_url):
        print(cur_url['url'])
        await semaphore.acquire()
        async with limiter:
            final_url = url_api_modifier(cur_url, api_key)
            # TODO try except, 
            #      status update for fail, 
            #      check response from process_and_store
            async with session.get(final_url) as response:
                http_code = response.status
                logging.info('%s response code %d', cur_url['url'], http_code)
                if http_code == 200:
                    logging.debug('Calling function %s with store path : %s', process_and_store.__name__, cur_url['store_path'])
                    await process_and_store(response, cur_url['store_path'])
                    cur_url['status'] = 'ok'
                    cur_url['http_code'] = str(http_code)
                else:
                    cur_url['status'] = 'fail_http'
                    cur_url['http_code'] = str(http_code)
                    print("HTTP status code: "+str(http_code))
                semaphore.release()
                # return response

# async download
async def _async_download_url_list(url_list, url_api_modifier, api_key, process_and_store, rate_params):
    # create semaphore
    semaphore = asyncio.Semaphore(rate_params['max_parallel_req'])
    # limiter
    limiter = AsyncLimiter(rate_params['req_per_unit_time'], rate_params['unit_time'])
    # create session
    conn = aiohttp.TCPConnector(limit_per_host=rate_params['limit_per_host'])
    async with aiohttp.ClientSession(connector=conn) as session:
        # loop over each url
        fut_list = []
        for cur_url in url_list:
            fut_list.append(fetch(session, cur_url, semaphore, limiter, url_api_modifier, api_key, process_and_store))
        responses = asyncio.gather(*fut_list)
        await responses
            
def download_url_list(url_list, url_api_modifier, api_key, process_and_store, rate_params):
    logging.debug('Downloading url list of size %d', len(url_list))
    
    if not url_api_modifier:
        url_api_modifier = lambda u, a : u['url']

    if 'max_parallel_req' not in rate_params:
        rate_params['max_parallel_req'] = 500
    if 'limit_per_host' not in rate_params:
        rate_params['limit_per_host'] = 0
    if 'req_per_unit_time' not in rate_params:
        rate_params['req_per_unit_time'] = 50
    if 'unit_time' not in rate_params:
        rate_params['unit_time'] = 1

    start_t = time.time()
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(_async_download_url_list(url_list, url_api_modifier, api_key, process_and_store, rate_params))
    loop.run_until_complete(future)
    end_t = time.time()
    print("The time required to download", len(url_list), "URLs :", end_t-start_t)

    return len(get_pending_or_fail_url_list(url_list))