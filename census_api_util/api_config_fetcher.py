import grequests
import requests
import json
from absl import app
from absl import flags
import sys
import os
import time
import random

module_dir_ = os.path.dirname(__file__)
module_parentdir_ = os.path.join(module_dir_, '..')
sys.path.append(os.path.join(module_dir_, '..'))

FLAGS = flags.FLAGS

# flags.DEFINE_string('zip_path', None, 'Path to zip file downloaded from US Census')
# flags.DEFINE_string('csv_path', None, 'Path to csv file downloaded from US Census')
# flags.DEFINE_list('csv_path_list', None, 'List of paths to csv files downloaded from US Census')
# flags.DEFINE_string('spec_path', None, 'Path to config spec JSON file')
# flags.DEFINE_boolean('get_tokens', False, 'Produce a list of tokens from the input file/s')
# flags.DEFINE_boolean('get_columns', False, 'Produce a list of columns from the input file/s')
# flags.DEFINE_boolean('get_ignored_columns', False, 'Produce a list of columns ignored from the input file/s according to spec')
# flags.DEFINE_boolean('ignore_columns', False, 'Account for columns to be ignored according to the spec')
# flags.DEFINE_boolean('is_metadata', False, 'Parses the file assuming it is _metadata_ type file')
# flags.DEFINE_string('delimiter', '!!', 'The delimiter to extract tokens from column name')


def request_url_json(url):
	print(req.url)
	try:
		req = requests.get(url)
	except requests.exceptions.ReadTimeout:
		time.sleep(10)
		req = requests.get(url)

	if req.status_code == requests.codes.ok:
		response_data = req.json()
		#print(response_data)
	else:
		response_data = {'http_err_code': req.status_code}
		print("HTTP status code: "+str(req.status_code))
		#if req.status_code != 204:
			#TODO
	return response_data

def _generate_url_prefix(link_tree, year=None):
	if year:
		return f'https://api.census.gov/data/{year}/{link_tree}/'
	else:
		return f'https://api.census.gov/data/{link_tree}/'

def generate_url_geography(link_tree, year=None):
	return _generate_url_prefix(link_tree, year)+'geography.json'

def generate_url_groups(link_tree, year=None):
	return _generate_url_prefix(link_tree, year)+'groups.json'

def generate_url_variables(link_tree, year=None):
	return _generate_url_prefix(link_tree, year)+'variables.json'

def generate_url_tags(link_tree, year=None):
	return _generate_url_prefix(link_tree, year)+'tags.json'

def generate_url_group_variables(link_tree, group_id, year=None):
	return _generate_url_prefix(link_tree, year)+f'groups/{group_id}.json'

def fetch_link_tree_config(store_path=module_dir_+'config_files/', force_fetch=False):
	store_path = os.path.expanduser(store_path)
	if not os.path.exists(store_path):
		os.makedirs(store_path, exist_ok=True)
	if force_fetch or not os.path.isfile(os.path.join(store_path, 'dataset_list.json')):
		datasets = request_url_json('https://api.census.gov/data.json')
		if 'http_err_code' not in datasets:
			with open(os.path.join(store_path, 'dataset_list.json'), 'w') as fp:
				json.dump(datasets, fp, indent=2)
	else:
		datasets = json.load(open(os.path.join(store_path, 'dataset_list.json'), 'r'))

	return datasets

def compile_year_map(store_path=module_dir_+'config_files/', force_fetch=False):

	if os.path.isfile(os.path.join(store_path, 'dataset_year.json')):
		link_tree_dict = json.load(open(os.path.join(store_path, 'dataset_year.json'), 'r'))
	else:
		datasets = fetch_link_tree_config(store_path, force_fetch)
		link_tree_dict = {}
		error_dict = {}
		for dataset_dict in datasets['dataset']:
			link_tree = '/'.join(dataset_dict['c_dataset'])
			if dataset_dict['c_isAvailable']:
				if link_tree not in link_tree_dict:
					link_tree_dict[link_tree] = {}
					link_tree_dict[link_tree]['years'] = {}

				identifier = dataset_dict['identifier']
				identifier = identifier[identifier.rfind('/')+1:]

				if 'c_vintage' in dataset_dict:
					year = dataset_dict['c_vintage']
					link_tree_dict[link_tree]['years'][year] = {}
					link_tree_dict[link_tree]['years'][year]['title'] = dataset_dict['title']
					link_tree_dict[link_tree]['years'][year]['identifier'] = identifier

				elif 'c_isTimeseries' in dataset_dict and dataset_dict['c_isTimeseries']:
					year = None

					if 'title' not in link_tree_dict[link_tree]:
						link_tree_dict[link_tree]['title'] = dataset_dict['title']
					elif link_tree_dict[link_tree]['title'] != dataset_dict['title']:
						if 'timeseries_multiple_titles' not in error_dict:
							error_dict['timeseries_multiple_titles'] = []
						error_dict['timeseries_multiple_titles'].append(link_tree)
						print(link_tree, 'found multiple title')

					if 'identifier' not in link_tree_dict[link_tree]:
						link_tree_dict[link_tree]['identifier'] = identifier
					elif link_tree_dict[link_tree]['identifier'] != identifier:
						if 'timeseries_multiple_identifiers' not in error_dict:
							error_dict['timeseries_multiple_identifiers'] = []
						error_dict['timeseries_multiple_identifiers'].append(link_tree)
						print(link_tree, 'found multiple identifiers')
				else:
					year = None
					if 'linktree_unkown_type' not in error_dict:
						error_dict['linktree_unkown_type'] = []
					error_dict['linktree_unkown_type'].append(link_tree)
					print('/'.join(dataset_dict['c_dataset']), "year not available and not timeseries")	

				if dataset_dict['distribution'][0]['accessURL'] != _generate_url_prefix(link_tree, year)[:-1]:
					if 'url_mismatch' not in error_dict:
						error_dict['url_mismatch'] = []
					error_dict['url_mismatch'].append({"expected": _generate_url_prefix(link_tree, year)[:-1], "actual":dataset_dict['distribution'][0]['accessURL']})
					print(link_tree, 'accessURL unexpected')
				
				if dataset_dict['c_geographyLink'] != generate_url_geography(link_tree, year):
					if 'url_mismatch' not in error_dict:
						error_dict['url_mismatch'] = []
					error_dict['url_mismatch'].append({"expected": generate_url_geography(link_tree, year), "actual":dataset_dict['c_geographyLink']})
					print(link_tree, 'c_geographyLink unexpected')

				if dataset_dict['c_groupsLink'] != generate_url_groups(link_tree, year):
					if 'url_mismatch' not in error_dict:
						error_dict['url_mismatch'] = []
					error_dict['url_mismatch'].append({"expected": generate_url_groups(link_tree, year), "actual":dataset_dict['c_groupsLink']})
					print(link_tree, 'c_groupsLink unexpected')

				if dataset_dict['c_variablesLink'] != generate_url_variables(link_tree, year):
					if 'url_mismatch' not in error_dict:
						error_dict['url_mismatch'] = []
					error_dict['url_mismatch'].append({"expected": generate_url_variables(link_tree, year), "actual":dataset_dict['c_variablesLink']})
					print(link_tree, 'c_variablesLink unexpected')


				if 'c_tagsLink' in dataset_dict:
					if dataset_dict['c_tagsLink'] != generate_url_tags(link_tree, year):
						if 'url_mismatch' not in error_dict:
							error_dict['url_mismatch'] = []
						error_dict['url_mismatch'].append({"expected": generate_url_tags(link_tree, year), "actual":dataset_dict['c_tagsLink']})
						print(link_tree, 'c_tagsLink unexpected')

				if len(dataset_dict['distribution']) > 1:
					if 'link_tree_multiple_distribution' not in error_dict:
						error_dict['link_tree_multiple_distribution'] = []
					error_dict['link_tree_multiple_distribution'].append(link_tree)
					print(link_tree, "has multiple distribution")

				if 'c_tagsLink' not in dataset_dict:
					if 'missing_tags' not in error_dict:
						error_dict['missing_tags'] = []
					error_dict['missing_tags'].append(link_tree)
					print(link_tree, "c_tagsLink not present")
			else:
				if 'unavailable_link_trees' not in error_dict:
					error_dict['unavailable_link_trees'] = []
				error_dict['unavailable_link_trees'].append(link_tree)
				print(link_tree, "not available")
		
		with open(os.path.join(store_path, 'dataset_year.json'), 'w') as fp:
			json.dump(link_tree_dict, fp, indent=2)
		if error_dict:
			with open(os.path.join(store_path, 'errors_dataset_year.json'), 'w') as fp:
				json.dump(error_dict, fp, indent=2)

	return link_tree_dict

def compile_groups_map(store_path=module_dir_+'config_files/', force_fetch=False):
	if os.path.isfile(os.path.join(store_path, 'dataset_groups.json')) and not force_fetch:
		link_tree_dict = json.load(open(os.path.join(store_path, 'dataset_groups.json'), 'r'))
	else:
		link_tree_dict = compile_year_map(store_path, force_fetch)
		error_dict = {}
		
		for link_tree in link_tree_dict:
			if 'years' in link_tree_dict[link_tree]:
				for year in link_tree_dict[link_tree]['years']:
					temp_url = generate_url_groups(link_tree, year)
					group_list = request_url_json(temp_url)
					if 'http_err_code' not in group_list:
						if len(group_list) != 1:
							if 'groups_extra_keys' not in error_dict:
								error_dict['groups_extra_keys'] = []
							error_dict['groups_extra_keys'].append(temp_url)
							print(temp_url, "has unexpected number of keys ")
						group_list = group_list['groups']
						link_tree_dict[link_tree]['years'][year]['groups'] = {}
						for cur_group in group_list:
							link_tree_dict[link_tree]['years'][year]['groups'][cur_group['name']] = {}
							link_tree_dict[link_tree]['years'][year]['groups'][cur_group['name']]['title'] = cur_group['description']
							# check only 3 key values
							if len(cur_group) != 3:
								if 'groups_extra_keys' not in error_dict:
									error_dict['groups_extra_keys'] = []
								error_dict['groups_extra_keys'].append(temp_url)
								print(temp_url, "has unexpected number of keys ")
							# check variables url
							if cur_group['variables'] != generate_url_group_variables(link_tree, cur_group['name'], year):
								if 'url_mismatch' not in error_dict:
									error_dict['url_mismatch'] = []
								error_dict['url_mismatch'].append({"expected": generate_url_group_variables(link_tree, cur_group['name'], year), "actual":cur_group['variables']})
								print(link_tree, 'group_variablesLink unexpected')

			else:
				temp_url = generate_url_groups(link_tree, year)
				group_list = request_url_json(temp_url)
				if 'http_err_code' not in group_list:
					if len(group_list) != 1:
						if 'groups_extra_keys' not in error_dict:
							error_dict['groups_extra_keys'] = []
						error_dict['groups_extra_keys'].append(temp_url)
						print(temp_url, "not available")
					group_list = group_list['groups']
					link_tree_dict[link_tree]['groups'] = {}
					for cur_group in group_list:
						link_tree_dict[link_tree]['groups'][cur_group['name']] = {}
						link_tree_dict[link_tree]['groups'][cur_group['name']]['title'] = cur_group['description']
						# check only 3 key values
						if len(cur_group) != 3:
							if 'groups_extra_keys' not in error_dict:
								error_dict['groups_extra_keys'] = []
							error_dict['groups_extra_keys'].append(temp_url)
							print(temp_url, "has unexpected number of keys ")
						# check variables url
						if cur_group['variables'] != generate_url_group_variables(link_tree, cur_group['name']):
							if 'url_mismatch' not in error_dict:
								error_dict['url_mismatch'] = []
							error_dict['url_mismatch'].append({"expected": generate_url_group_variables(link_tree, cur_group['name']), "actual":cur_group['variables']})
							print(link_tree, 'group_variablesLink unexpected')


		with open(os.path.join(store_path, 'dataset_groups.json'), 'w') as fp:
			json.dump(link_tree_dict, fp, indent=2)
		if error_dict:
			with open(os.path.join(store_path, 'errors_dataset_groups.json'), 'w') as fp:
				json.dump(error_dict, fp, indent=2)

	return link_tree_dict
	
def compile_geography_map(store_path=module_dir_+'config_files/', force_fetch=False):
	if os.path.isfile(os.path.join(store_path, 'dataset_geography.json')) and not force_fetch:
		link_tree_dict = json.load(open(os.path.join(store_path, 'dataset_geography.json'), 'r'))
	else:
		link_tree_dict = compile_groups_map(store_path, force_fetch)
		error_dict = {}
		
		for link_tree in link_tree_dict:
			if 'years' in link_tree_dict[link_tree]:
				for year in link_tree_dict[link_tree]['years']:
					temp_url = generate_url_geography(link_tree, year)
					geo_list = request_url_json(temp_url)
					if 'http_err_code' not in geo_list:
						if len(geo_list) != 1:
							if 'groups_extra_keys' not in error_dict:
								error_dict['groups_extra_keys'] = []
							error_dict['groups_extra_keys'].append(temp_url)
							print(temp_url, "has unexpected number of keys ")
						if 'fips' in geo_list:
							geo_list = geo_list['fips']
							link_tree_dict[link_tree]['years'][year]['geos'] = {}
							for cur_geo in geo_list:
								if cur_geo['name'] not in link_tree_dict[link_tree]['years'][year]['geos']:
									link_tree_dict[link_tree]['years'][year]['geos'][cur_geo['name']] = {}
									if 'geoLevelDisplay' in cur_geo:
										link_tree_dict[link_tree]['years'][year]['geos'][cur_geo['name']]['geoLevelId'] = cur_geo['geoLevelDisplay']
									elif 'geoLevelId' in cur_geo:
										link_tree_dict[link_tree]['years'][year]['geos'][cur_geo['name']]['geoLevelId'] = cur_geo['geoLevelId']
									else:
										if 'geo_missing_id' not in error_dict:
											error_dict['geo_missing_id'] = []
										error_dict['geo_missing_id'].append(temp_url+" "+cur_geo['name'])
										print(cur_geo['name'], "has no geoId ")
									
									if 'geoLevelDisplay' in cur_geo and 'geoLevelId' in cur_geo and cur_geo['geoLevelDisplay'] != cur_geo['geoLevelId']:
										if 'geo_multiple_id' not in error_dict:
											error_dict['geo_multiple_id'] = []
										error_dict['geo_multiple_id'].append(temp_url+" "+cur_geo['name'])
										print(cur_geo['name'], "has multiple geoId ")

									if 'requires' in cur_geo:
										link_tree_dict[link_tree]['years'][year]['geos'][cur_geo['name']]['requires'] = cur_geo['requires']
									if 'wildcard' in cur_geo:
										link_tree_dict[link_tree]['years'][year]['geos'][cur_geo['name']]['wildcard'] = cur_geo['wildcard']
									if 'limit' in cur_geo:
										link_tree_dict[link_tree]['years'][year]['geos'][cur_geo['name']]['limit'] = cur_geo['limit']
									# check only 3 key values
									if len(cur_geo) > 7:
										if 'groups_extra_keys' not in error_dict:
											error_dict['groups_extra_keys'] = []
										error_dict['groups_extra_keys'].append(temp_url)
										print(temp_url, "has unexpected number of keys ")
								else:
									if 'geo_conflicts' not in error_dict:
										error_dict['geo_conflicts'] = []
									error_dict['geo_conflicts'].append('_'.join(link_tree, year, cur_geo['name']))
									print(temp_url, "has unexpected number of keys ")
						else:
							if 'fips_missing' not in error_dict:
								error_dict['fips_missing'] = []
							error_dict['fips_missing'].append(temp_url)
							print(temp_url, "fips missing")

			else:
				temp_url = generate_url_geography(link_tree, year)
				geo_list = request_url_json(temp_url)
				if 'http_err_code' not in geo_list:
					if len(geo_list) != 1:
						if 'groups_extra_keys' not in error_dict:
							error_dict['groups_extra_keys'] = []
						error_dict['groups_extra_keys'].append(temp_url)
						print(temp_url, "not available")
					if 'fips' in geo_list:
						geo_list = geo_list['fips']
						link_tree_dict[link_tree]['geos'] = {}
						for cur_geo in geo_list:
							if cur_geo['name'] not in link_tree_dict[link_tree]['years'][year]['geos']:
								link_tree_dict[link_tree]['geos'][cur_geo['name']] = {}
								if 'geoLevelDisplay' in cur_geo:
									link_tree_dict[link_tree]['geos'][cur_geo['name']]['geoLevelId'] = cur_geo['geoLevelDisplay']
								elif 'geoLevelId' in cur_geo:
									link_tree_dict[link_tree]['geos'][cur_geo['name']]['geoLevelId'] = cur_geo['geoLevelId']
								else:
									if 'geo_missing_id' not in error_dict:
										error_dict['geo_missing_id'] = []
									error_dict['geo_missing_id'].append(temp_url+" "+cur_geo['name'])
									print(cur_geo['name'], "has no geoId ")
								
								if 'geoLevelDisplay' in cur_geo and 'geoLevelId' in cur_geo and cur_geo['geoLevelDisplay'] != cur_geo['geoLevelId']:
									if 'geo_multiple_id' not in error_dict:
										error_dict['geo_multiple_id'] = []
									error_dict['geo_multiple_id'].append(temp_url+" "+cur_geo['name'])
									print(cur_geo['name'], "has multiple geoId ")
								
								if 'requires' in cur_geo:
									link_tree_dict[link_tree]['geos'][cur_geo['name']]['requires'] = cur_geo['requires']
								if 'wildcard' in cur_geo:
									link_tree_dict[link_tree]['geos'][cur_geo['name']]['wildcard'] = cur_geo['wildcard']
								if 'limit' in cur_geo:
									link_tree_dict[link_tree]['geos'][cur_geo['name']]['limit'] = cur_geo['limit']
								# check only 3 key values
								if len(cur_geo) > 7:
									if 'groups_extra_keys' not in error_dict:
										error_dict['groups_extra_keys'] = []
									error_dict['groups_extra_keys'].append(temp_url)
									print(temp_url, "has unexpected number of keys ")
							else:
								if 'geo_conflicts' not in error_dict:
									error_dict['geo_conflicts'] = []
								error_dict['geo_conflicts'].append('_'.join(link_tree, cur_geo['name']))
								print(temp_url, "has unexpected number of keys ")
					else:
						if 'fips_missing' not in error_dict:
							error_dict['fips_missing'] = []
						error_dict['fips_missing'].append(temp_url)
						print(temp_url, "fips missing")

		with open(os.path.join(store_path, 'dataset_geography.json'), 'w') as fp:
			json.dump(link_tree_dict, fp, indent=2)
		if error_dict:
			with open(os.path.join(store_path, 'errors_dataset_geography.json'), 'w') as fp:
				json.dump(error_dict, fp, indent=2)

	return link_tree_dict

def compile_non_group_variables_map(store_path=module_dir_+'config_files/', force_fetch=False):
	if os.path.isfile(os.path.join(store_path, 'dataset_non_group_variables.json')) and not force_fetch:
		link_tree_dict = json.load(open(os.path.join(store_path, 'dataset_non_group_variables.json'), 'r'))
	else:
		link_tree_dict = compile_geography_map(store_path, force_fetch)
		error_dict = {}
		
		for link_tree in link_tree_dict:
			if 'years' in link_tree_dict[link_tree]:
				for year in link_tree_dict[link_tree]['years']:
					temp_url = generate_url_variables(link_tree, year)
					variable_list = request_url_json(temp_url)
					if 'http_err_code' not in variable_list:
						if len(variable_list) != 1:
							if 'groups_extra_keys' not in error_dict:
								error_dict['groups_extra_keys'] = []
							error_dict['groups_extra_keys'].append(temp_url)
							print(temp_url, "has unexpected number of keys ")
						if 'variables' in variable_list:
							variable_list = variable_list['variables']
							link_tree_dict[link_tree]['years'][year]['variables'] = {}
							for cur_variable in variable_list:
								if 'group' not in variable_list[cur_variable] or variable_list[cur_variable]['group'] == 'N/A':
									link_tree_dict[link_tree]['years'][year]['variables'][cur_variable] = {}
									link_tree_dict[link_tree]['years'][year]['variables'][cur_variable]['label'] = variable_list[cur_variable]['label']
									if 'concept' in variable_list[cur_variable]:
										link_tree_dict[link_tree]['years'][year]['variables'][cur_variable]['concept'] = variable_list[cur_variable]['concept']
									if 'predicateType' in variable_list[cur_variable]:
										link_tree_dict[link_tree]['years'][year]['variables'][cur_variable]['predicateType'] = variable_list[cur_variable]['predicateType']
						else:
							if 'variables_missing' not in error_dict:
								error_dict['variables_missing'] = []
							error_dict['variables_missing'].append(temp_url)
							print(temp_url, "has no variables section")
			else:
				temp_url = generate_url_variables(link_tree, year)
				variable_list = request_url_json(temp_url)
				if 'http_err_code' not in variable_list:
					if len(variable_list) != 1:
						if 'groups_extra_keys' not in error_dict:
							error_dict['groups_extra_keys'] = []
						error_dict['groups_extra_keys'].append(temp_url)
						print(temp_url, "has unexpected number of keys ")
					if 'variables' in variable_list:
						variable_list = variable_list['variables']
						link_tree_dict[link_tree]['variables'] = {}
						for cur_variable in variable_list:
							if 'group' not in variable_list[cur_variable] or variable_list[cur_variable]['group'] == 'N/A':
								link_tree_dict[link_tree]['variables'][cur_variable] = {}
								link_tree_dict[link_tree]['variables'][cur_variable]['label'] = variable_list[cur_variable]['label']
								if 'concept' in variable_list[cur_variable]:
									link_tree_dict[link_tree]['variables'][cur_variable]['concept'] = variable_list[cur_variable]['concept']
								if 'predicateType' in variable_list[cur_variable]:
									link_tree_dict[link_tree]['variables'][cur_variable]['predicateType'] = variable_list[cur_variable]['predicateType']
					else:
						if 'variables_missing' not in error_dict:
							error_dict['variables_missing'] = []
						error_dict['variables_missing'].append(temp_url)
						print(temp_url, "has no variables section")
		with open(os.path.join(store_path, 'dataset_non_group_variables.json'), 'w') as fp:
			json.dump(link_tree_dict, fp, indent=2)
		if error_dict:
			with open(os.path.join(store_path, 'errors_dataset_non_group_variables.json'), 'w') as fp:
				json.dump(error_dict, fp, indent=2)

	return link_tree_dict

def compile_link_tree_based_map(store_path=module_dir_+'config_files/', force_fetch=False):
	# compile_year_map(store_path)
	# compile_groups_map(store_path, force_fetch)
	# compile_geography_map(store_path, force_fetch)
	link_tree_dict = compile_non_group_variables_map(store_path, force_fetch)
	# link_tree_dict = compile_group_variables_map(store_path, force_fetch)

def fetch_group_variables(store_path=module_dir_+'config_files/', force_fetch=False):
	
	link_tree_dict = compile_non_group_variables_map(store_path, force_fetch)
	
	error_dict = {}
	url_list = []
	
	variables_path = os.path.join(store_path, 'group_variables')
	if not os.path.exists(variables_path):
		os.makedirs(variables_path, exist_ok=True)
	
	for link_tree in link_tree_dict:
		if 'years' in link_tree_dict[link_tree]:
			for year in link_tree_dict[link_tree]['years']:
				for group_id in link_tree_dict[link_tree]['years'][year]['groups']:
					temp_url = generate_url_group_variables(link_tree, group_id, year)
					file_path = os.path.join(variables_path, link_tree, str(year))
					file_name = os.path.join(file_path, group_id+'.json')
					if not os.path.exists(file_path):
						os.makedirs(file_path, exist_ok=True)
					url_list.append({'url':temp_url, 'file_name': file_name})
					
		else:
			for group_id in link_tree_dict[link_tree]['groups']:
				temp_url = generate_url_group_variables(link_tree, group_id)
				file_path = os.path.join(variables_path, link_tree)
				file_name = os.path.join(file_path, group_id+'.json')
				if not os.path.exists(file_path):
					os.makedirs(file_path, exist_ok=True)
				url_list.append({'url':temp_url, 'file_name': file_name})
	
	

	temp_list = url_list
	
	if not force_fetch:
		temp_list = []
		for cur_url in url_list:
			if not os.path.isfile(os.path.join(cur_url['file_name'])):
				temp_list.append(cur_url)
	print(len(temp_list))
	while len(temp_list) > 0:
		n = 50
		urls_chunked = [temp_list[i:i + n] for i in range(0, len(temp_list), n)]

		for cur_chunk in urls_chunked:
			results = grequests.map((grequests.get(u['url']) for u in cur_chunk), size=n)
			# store outputs to files under group_variables folder
			for i, resp in enumerate(results):
				if resp:
					print(cur_chunk[i]['url'])
					for url_temp in url_list:
						if url_temp['url'] == cur_chunk[i]['url']:
							temp_list.remove(url_temp)
					if resp.status_code == 200:
						resp_data = resp.json()
						with open(cur_chunk[i]['file_name'], 'w') as fp:
							json.dump(resp_data, fp, indent=2)
					else:
						print(resp.status_code, cur_chunk[i]['url'])
						if 'request_error' not in error_dict:
							error_dict['request_error'] = []
						error_dict['request_error'].append(cur_chunk[i]['url'])
			
			# delay 1 s
			time.sleep(0.8 + (random.random() / 2 ))

	if error_dict:
		with open(os.path.join(store_path, 'errors_dataset_group_variables.json'), 'w') as fp:
			json.dump(error_dict, fp, indent=2)

	return link_tree_dict

def fetch_group_variables_map(store_path=module_dir_+'config_files/', force_fetch=False):
	if os.path.isfile(os.path.join(store_path, 'dataset_config.json')) and not force_fetch:
		link_tree_dict = json.load(open(os.path.join(store_path, 'dataset_config.json'), 'r'))
	else:
		link_tree_dict = fetch_group_variables(store_path, force_fetch)
		error_dict = {}
		
		# for link_tree in link_tree_dict:
		# 	if 'years' in link_tree_dict[link_tree]:
		# 		for year in link_tree_dict[link_tree]['years']:
		# 			for group_id in link_tree_dict[link_tree]['years'][year]['groups']:
		# 				temp_url = generate_url_group_variables(link_tree, group_id, year)
		# 				variable_list = request_url_json(temp_url)

		# 				if 'http_err_code' not in variable_list:
		# 					if len(variable_list) != 1:
		# 						if 'groups_extra_keys' not in error_dict:
		# 							error_dict['groups_extra_keys'] = []
		# 						error_dict['groups_extra_keys'].append(temp_url)
		# 						print(temp_url, "has unexpected number of keys ")
		# 					if 'variables' in variable_list:
		# 						variable_list = variable_list['variables']
		# 						link_tree_dict[link_tree]['years'][year]['groups'][group_id]['variables'] = {}
		# 						for cur_variable in variable_list:
		# 							link_tree_dict[link_tree]['years'][year]['groups'][group_id]['variables'][variable_list[cur_variable]['label']] = {}
		# 							link_tree_dict[link_tree]['years'][year]['groups'][group_id]['variables'][variable_list[cur_variable]['label']]['id'] = cur_variable
		# 							if 'predicateType' in variable_list[cur_variable]:
		# 								link_tree_dict[link_tree]['years'][year]['groups'][group_id]['variables'][variable_list[cur_variable]['label']]['predicateType'] = variable_list[cur_variable]['predicateType']
		# 					else:
		# 						if 'variables_missing' not in error_dict:
		# 							error_dict['variables_missing'] = []
		# 						error_dict['variables_missing'].append(temp_url)
		# 						print(temp_url, "has no variables section")
		# 	else:
		# 		for group_id in link_tree_dict[link_tree]['groups']:
		# 			temp_url = generate_url_group_variables(link_tree, group_id, year)
		# 			variable_list = request_url_json(temp_url)
		# 			if 'http_err_code' not in variable_list:
		# 				if len(variable_list) != 1:
		# 					if 'groups_extra_keys' not in error_dict:
		# 						error_dict['groups_extra_keys'] = []
		# 					error_dict['groups_extra_keys'].append(temp_url)
		# 					print(temp_url, "has unexpected number of keys ")
		# 				if 'variables' in variable_list:
		# 					variable_list = variable_list['variables']
		# 					link_tree_dict[link_tree]['groups'][group_id]['variables'] = {}
		# 					for cur_variable in variable_list:
		# 						link_tree_dict[link_tree]['groups'][group_id]['variables'][cur_variable['label']] = {}
		# 						link_tree_dict[link_tree]['groups'][group_id]['variables'][cur_variable['label']]['id'] = cur_variable
		# 						if 'predicateType' in variable_list[cur_variable]:
		# 							link_tree_dict[link_tree]['groups'][group_id]['variables'][cur_variable['label']]['predicateType'] = variable_list[cur_variable]['predicateType']
		# 				else:
		# 					if 'variables_missing' not in error_dict:
		# 						error_dict['variables_missing'] = []
		# 					error_dict['variables_missing'].append(temp_url)
		# 					print(temp_url, "has no variables section")
	
		# with open(os.path.join(store_path, 'dataset_config.json'), 'w') as fp:
		# 	json.dump(link_tree_dict, fp, indent=2)
		# if error_dict:
		# 	with open(os.path.join(store_path, 'errors_dataset_config.json'), 'w') as fp:
		# 		json.dump(error_dict, fp, indent=2)

	return link_tree_dict


def compile_group_based_map():
	pass
# wrapper functions for finding available options


compile_link_tree_based_map()
fetch_group_variables()

	