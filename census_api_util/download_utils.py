import os
import json
import time
import random
import aiohttp
import asyncio

# https://stackoverflow.com/questions/41691327/ssl-sslerror-ssl-certificate-verify-failed-certificate-verify-failed-ssl-c/41692664 

def url_list_check_downloaded(url_list, force_fetch=False):
  for cur_url in url_list:
    if os.path.isfile(os.path.join(url_list[cur_url]['output_path'])):
      url_list[cur_url]['status'] = 'saved'
    elif 'status' not in url_list[cur_url]:
      url_list[cur_url]['status'] = 'failed'
    if force_fetch:
      url_list[cur_url]['status'] = 'failed'

  return url_list

async def download_url(session, cur_url, url_list_dict, error_dict):
  print(cur_url)
  async with session.get(cur_url) as resp:
    if resp:
      print(cur_url)
      url_list_dict[cur_url]['http_code'] = resp.status
      if resp.status == 200:
        resp_data = await resp.json()
        # TODO create folder structure if it doesn't exist
        with open(url_list_dict[cur_url]['output_path'], 'w') as fp:
          json.dump(resp_data, fp, indent=2)
        url_list_dict[cur_url]['status'] = 'saved'
      else:
        print(resp.status, cur_url)
        url_list_dict[cur_url]['status'] = 'http_error'

        if 'http_error' not in error_dict:
          error_dict['http_error'] = []
        error_dict['http_error'].append(cur_url)
    else:
      url_list_dict[cur_url]['status'] = 'failed'

      if 'request_error' not in error_dict:
        error_dict['request_error'] = []
      error_dict['request_error'].append(cur_url)

  return url_list_dict[cur_url]

async def download_all_chunks(url_list_dict, urls_chunked, error_dict, status_file, chunk_delay_s=1):
  tasks = []
  async with aiohttp.ClientSession() as session:
    for cur_chunk in urls_chunked:
      for cur_url in cur_chunk:
        task = asyncio.ensure_future(download_url(session, cur_url, url_list_dict, error_dict))
        tasks.append(task)
      responses = await asyncio.gather(*tasks)
      # delay 1 s
      time.sleep(chunk_delay_s + (random.random() / 2))
      # TODO update status file here
      with open(status_file, 'w') as fp:
        json.dump(url_list_dict, fp, indent=2)

def dowload_url_list_parallel(status_file, chunk_size=30, chunk_delay_s=1):

  status_file = os.path.expanduser(status_file)

  url_list_dict = json.load(open(status_file, 'r'))
  url_list = list(url_list_dict.keys())

  # remove URLs with non failed status
  temp_list = url_list.copy()

  for _url in temp_list:
    if url_list_dict[_url]['status'] != 'failed':
      url_list.remove(_url)

  urls_chunked = [
      url_list[i:i + chunk_size] for i in range(0, len(url_list), chunk_size)
  ]

  error_dict = {}

  loop = asyncio.get_event_loop()
  future = asyncio.ensure_future(download_all_chunks(url_list_dict, urls_chunked, error_dict, status_file, chunk_delay_s))
  loop.run_until_complete(future)

  with open(status_file, 'w') as fp:
    json.dump(url_list_dict, fp, indent=2)

  return error_dict
