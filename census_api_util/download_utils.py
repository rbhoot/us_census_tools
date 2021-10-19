import grequests
import os
import json
import time
import random

# TODO update status of existing files

def dowload_url_list_parallel(status_file, chunk_size = 30, chunk_delay_s = 1):
	
	status_file = os.path.expanduser(status_file)

	url_list_dict = json.load(open(status_file, 'r'))
	url_list = list(url_list_dict.keys())
	
	# remove URLs with non failed status
	temp_list = url_list.copy()

	for _url in temp_list:
		if url_list_dict[_url]['status'] != 'failed':
			url_list.remove(_url)

	urls_chunked = [url_list[i:i + chunk_size] for i in range(0, len(url_list), chunk_size)]

	error_dict = {}

	for cur_chunk in urls_chunked:
		results = grequests.map((grequests.get(u) for u in cur_chunk), size=chunk_size)
		# store outputs to files under group_variables folder
		for i, resp in enumerate(results):
			if resp:
				print(cur_chunk[i])
				url_list_dict[cur_chunk[i]]['http_code'] = resp.status_code
				
				if resp.status_code == 200:
					resp_data = resp.json()
					# TODO create folder structure if it doesn't exist
					with open(url_list_dict[cur_chunk[i]]['output_path'], 'w') as fp:
						json.dump(resp_data, fp, indent=2)
					url_list_dict[cur_chunk[i]]['status'] = 'saved'
				else:
					print(resp.status_code, cur_chunk[i])
					url_list_dict[cur_chunk[i]]['status'] = 'http_error'
					
					if 'http_error' not in error_dict:
						error_dict['http_error'] = []
					error_dict['http_error'].append(cur_chunk[i])
			else:
				url_list_dict[cur_chunk[i]]['status'] = 'failed'

				if 'request_error' not in error_dict:
					error_dict['request_error'] = []
				error_dict['request_error'].append(cur_chunk[i])
		
		# delay 1 s
		time.sleep(chunk_delay_s + (random.random() / 2 ))

	with open(status_file, 'w') as fp:
		json.dump(url_list_dict, fp, indent=2)
	
	return error_dict