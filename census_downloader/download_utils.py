# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Function library to make parallel requests and process the response.
"""

import json
import logging
import time
import asyncio
import aiohttp
from typing import Any, Callable, Union
from aiolimiter import AsyncLimiter

from status_file_utils import get_pending_or_fail_url_list, url_to_download

async def async_save_resp_json(resp: Any, store_path: str):
    resp_data = await resp.json()
    logging.debug('Writing downloaded data to file: %s', store_path)
    json.dump(resp_data, open(store_path, 'w'), indent = 2)

def default_url_filter(url_list: list) -> list:
    ret_list = []
    for cur_url in url_list:
        if cur_url['status'] == 'pending' or cur_url['status'].startswith('fail'):
            ret_list.append(cur_url)
    return ret_list

def download_url_list_iterations(url_list: list, url_api_modifier: Callable[[dict], str], api_key: str, process_and_store: Callable[[Any, str], int], url_filter: Union[Callable[[list], list], None] = None, max_itr: int = 3, rate_params: dict = {}) -> int:
    loop_ctr = 0
    if not url_filter:
        url_filter = default_url_filter
    logging.info('downloading URLs')
    
    cur_url_list = url_filter(url_list)
    failed_urls_ctr = len(url_list)
    prev_failed_ctr = failed_urls_ctr + 1
    while failed_urls_ctr > 0 and loop_ctr < max_itr and prev_failed_ctr > failed_urls_ctr:
        prev_failed_ctr = failed_urls_ctr
        logging.info('downloading URLs iteration:%d', loop_ctr)
        download_url_list(cur_url_list, url_api_modifier, api_key, process_and_store, rate_params)
        cur_url_list = url_filter(url_list)
        failed_urls_ctr = len(cur_url_list)
        logging.info('failed request count: %d', failed_urls_ctr)
        loop_ctr += 1
    return failed_urls_ctr

# req url
# TODO add back off decorator with aiohttp.ClientConnectionError as the trigger exception, 
#      try except might need to change for decorator to work
async def fetch(session: Any, cur_url: str, semaphore: Any, limiter: Any, url_api_modifier: Callable[[dict], str], api_key: str, process_and_store: Callable[[Any, str], int]):
# async def fetch(session, url, semaphore):
    if url_to_download(cur_url):
        print(cur_url['url'])
        await semaphore.acquire()
        async with limiter:
            final_url = url_api_modifier(cur_url, api_key)
            try:
                # TODO allow other methods like POST
                async with session.get(final_url) as response:
                    http_code = response.status
                    logging.info('%s response code %d', cur_url['url'], http_code)
                    # TODO allow custom call back function that returns boolean value for success
                    if http_code == 200:
                        logging.debug('Calling function %s with store path : %s', process_and_store.__name__, cur_url['store_path'])
                        store_ret = await process_and_store(response, cur_url['store_path'])
                        if store_ret < 0:
                            cur_url['status'] = 'fail'
                        else:    
                            cur_url['status'] = 'ok'
                        cur_url['http_code'] = str(http_code)
                    else:
                        cur_url['status'] = 'fail_http'
                        cur_url['http_code'] = str(http_code)
                        print("HTTP status code: "+str(http_code))
                    semaphore.release()
                    # return response
            except Exception as e:
                cur_url['status'] = 'fail'
                cur_url.pop('http_code', None)
                logging.error('%s failed fetch with exception %s', cur_url['url'], type(e).__name__)



# async download
async def _async_download_url_list(url_list: list, url_api_modifier: Callable[[dict], str], api_key: str, process_and_store: Callable[[Any, str], int], rate_params: dict):
    # create semaphore
    semaphore = asyncio.Semaphore(rate_params['max_parallel_req'])
    # limiter
    limiter = AsyncLimiter(rate_params['req_per_unit_time'], rate_params['unit_time'])
    # create session
    conn = aiohttp.TCPConnector(limit_per_host=rate_params['limit_per_host'])
    timeout = aiohttp.ClientTimeout(total=1200)
    async with aiohttp.ClientSession(connector=conn, timeout=timeout) as session:
        # loop over each url
        fut_list = []
        for cur_url in url_list:
            fut_list.append(fetch(session, cur_url, semaphore, limiter, url_api_modifier, api_key, process_and_store))
        responses = asyncio.gather(*fut_list)
        # TODO update download_status file at regular intervals if feasible
        await responses
            
def download_url_list(url_list: list, url_api_modifier: Callable[[dict], str], api_key: str, process_and_store: Callable[[Any, str], int], rate_params: dict):
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