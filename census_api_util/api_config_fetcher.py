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
config_path_ = os.path.join(module_dir_, 'config_files')
sys.path.append(os.path.join(module_dir_, '..'))

from census_api_util.download_utils import *

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
	print(url)
	try:
		req = requests.get(url)
		# print(req.url)
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

def fetch_link_tree_config(store_path=config_path_, force_fetch=False):
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

def compile_year_map(store_path=config_path_, force_fetch=False):

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

def fetch_linktree_config_cache(param, store_path=config_path_, force_fetch=False):
	if param not in ['groups', 'geography', 'variables', 'group_variables']:
		error_dict = {'invalid_param': [param]}
		with open(os.path.join(store_path, f'errors_dataset_{param}_download.json'), 'w') as fp:
			json.dump(error_dict, fp, indent=2)
		return
	
	store_path = os.path.abspath(store_path)

	link_tree_dict = compile_year_map(store_path, force_fetch)
	if param == 'group_variables':
		link_tree_dict = compile_non_group_variables_map(store_path, force_fetch)
	error_dict = {}
	url_list = {}
	
	cache_path = os.path.join(store_path, 'api_cache')
	if not os.path.exists(cache_path):
		os.makedirs(cache_path, exist_ok=True)
	
	status_file = os.path.join(cache_path, f'{param}_cache_status.json')
	
	if os.path.isfile(status_file):
		url_list = json.load(open(status_file, 'r'))
	
	for link_tree in link_tree_dict:
		if 'years' in link_tree_dict[link_tree]:
			for year in link_tree_dict[link_tree]['years']:
				if param == 'groups':
					temp_url = generate_url_groups(link_tree, year)
				elif param == 'geography':
					temp_url = generate_url_geography(link_tree, year)
				elif param == 'variables':
					temp_url = generate_url_variables(link_tree, year)
				else:
					temp_url = None
				
				file_path = os.path.join(cache_path, link_tree, str(year))
				file_name = os.path.join(file_path, f'{param}.json')
				if not os.path.exists(file_path):
					os.makedirs(file_path, exist_ok=True)
				
				if temp_url and temp_url not in url_list:
					url_list[temp_url] = {}
					url_list[temp_url]['output_path'] = file_name
					url_list[temp_url]['status'] = 'failed'

				if param == 'group_variables':
					for group_id in link_tree_dict[link_tree]['years'][year]['groups']:
						temp_url = generate_url_group_variables(link_tree, group_id, year)
						file_name = os.path.join(file_path, group_id+'.json')
						if temp_url not in url_list:
							url_list[temp_url] = {}
							url_list[temp_url]['output_path'] = file_name
							url_list[temp_url]['status'] = 'failed'
					
		else:
			if param == 'groups':
				temp_url = generate_url_groups(link_tree)
			elif param == 'geography':
				temp_url = generate_url_geography(link_tree)
			elif param == 'variables':
				temp_url = generate_url_variables(link_tree)
			else:
				temp_url = None

			file_path = os.path.join(cache_path, link_tree)
			file_name = os.path.join(file_path, f'{param}.json')
			if not os.path.exists(file_path):
				os.makedirs(file_path, exist_ok=True)
			if temp_url and temp_url not in url_list:
				url_list[temp_url] = {}
				url_list[temp_url]['output_path'] = file_name
				url_list[temp_url]['status'] = 'failed'
			if param == 'group_variables':
				for group_id in link_tree_dict[link_tree]['groups']:
					temp_url = generate_url_group_variables(link_tree, group_id)
					file_name = os.path.join(file_path, group_id+'.json')
					if temp_url not in url_list:
						url_list[temp_url] = {}
						url_list[temp_url]['output_path'] = file_name
						url_list[temp_url]['status'] = 'failed'

	url_list = url_list_check_downloaded(url_list, force_fetch)
	
	status_file = os.path.join(cache_path, f'{param}_cache_status.json')
	
	with open(status_file, 'w') as fp:
		json.dump(url_list, fp, indent=2)

	print(len(url_list))

	error_dict = dowload_url_list_parallel(status_file, chunk_size = 50, chunk_delay_s = 0.8)

	if error_dict:
		with open(os.path.join(store_path, f'errors_dataset_{param}_download.json'), 'w') as fp:
			json.dump(error_dict, fp, indent=2)

def compile_groups_map(store_path=config_path_, force_fetch=False):
	if os.path.isfile(os.path.join(store_path, 'dataset_groups.json')) and not force_fetch:
		link_tree_dict = json.load(open(os.path.join(store_path, 'dataset_groups.json'), 'r'))
	else:
		link_tree_dict = compile_year_map(store_path, force_fetch)
		error_dict = {}
		fetch_linktree_config_cache('groups', store_path, force_fetch)
		cache_path = os.path.join(store_path, 'api_cache')
		for link_tree in link_tree_dict:
			if 'years' in link_tree_dict[link_tree]:
				for year in link_tree_dict[link_tree]['years']:
					cache_file = os.path.join(cache_path, link_tree, str(year), 'groups.json')
					temp_url = generate_url_groups(link_tree, year)
					if os.path.isfile(cache_file):
						group_list = json.load(open(cache_file, 'r'))
					else:
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
				cache_file = os.path.join(cache_path, link_tree, 'groups.json')
				temp_url = generate_url_groups(link_tree)
				if os.path.isfile(cache_file):
					group_list = json.load(open(cache_file, 'r'))
				else:
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
	
def compile_geography_map(store_path=config_path_, force_fetch=False):
	if os.path.isfile(os.path.join(store_path, 'dataset_geography.json')) and not force_fetch:
		link_tree_dict = json.load(open(os.path.join(store_path, 'dataset_geography.json'), 'r'))
	else:
		link_tree_dict = compile_groups_map(store_path, force_fetch)
		error_dict = {}
		fetch_linktree_config_cache('geography', store_path, force_fetch)
		cache_path = os.path.join(store_path, 'api_cache')
		for link_tree in link_tree_dict:
			if 'years' in link_tree_dict[link_tree]:
				for year in link_tree_dict[link_tree]['years']:
					cache_file = os.path.join(cache_path, link_tree, str(year), 'geography.json')
					temp_url = generate_url_geography(link_tree, year)
					if os.path.isfile(cache_file):
						geo_list = json.load(open(cache_file, 'r'))
					else:
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
									error_dict['geo_conflicts'].append('_'.join([link_tree, year, cur_geo['name']]))
									print(cur_geo['name'], "geo name conflict")
						else:
							if 'fips_missing' not in error_dict:
								error_dict['fips_missing'] = []
							error_dict['fips_missing'].append(temp_url)
							print(temp_url, "fips missing")

			else:
				cache_file = os.path.join(cache_path, link_tree, 'geography.json')
				temp_url = generate_url_geography(link_tree)
				if os.path.isfile(cache_file):
					geo_list = json.load(open(cache_file, 'r'))
				else:
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
								error_dict['geo_conflicts'].append('_'.join([link_tree, cur_geo['name']]))
								print(cur_geo['name'], "geo name conflict")
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

def compile_non_group_variables_map(store_path=config_path_, force_fetch=False):
	if os.path.isfile(os.path.join(store_path, 'dataset_non_group_variables.json')) and not force_fetch:
		link_tree_dict = json.load(open(os.path.join(store_path, 'dataset_non_group_variables.json'), 'r'))
	else:
		link_tree_dict = compile_geography_map(store_path, force_fetch)
		error_dict = {}
		fetch_linktree_config_cache('variables', store_path, force_fetch)
		cache_path = os.path.join(store_path, 'api_cache')
		for link_tree in link_tree_dict:
			if 'years' in link_tree_dict[link_tree]:
				for year in link_tree_dict[link_tree]['years']:
					cache_file = os.path.join(cache_path, link_tree, str(year), 'variables.json')
					temp_url = generate_url_variables(link_tree, year)
					if os.path.isfile(cache_file):
						variable_list = json.load(open(cache_file, 'r'))
					else:
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
				cache_file = os.path.join(cache_path, link_tree, year, 'variables.json')
				temp_url = generate_url_variables(link_tree)
				if os.path.isfile(cache_file):
					variable_list = json.load(open(cache_file, 'r'))
				else:
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

def compile_link_tree_based_map(store_path=config_path_, force_fetch=False):
	# compile_year_map(store_path)
	# compile_groups_map(store_path, force_fetch)
	# compile_geography_map(store_path, force_fetch)
	link_tree_dict = compile_non_group_variables_map(store_path, force_fetch)
	# link_tree_dict = compile_group_variables_map(store_path, force_fetch)

def compile_linktree_group_map(store_path=config_path_, force_fetch=False):
	if os.path.isfile(os.path.join(store_path, 'linktree_groups.json')) and not force_fetch:
		out_dict = json.load(open(os.path.join(store_path, 'linktree_groups.json'), 'r'))
	else:
		link_tree_dict = compile_non_group_variables_map(store_path, force_fetch)
		out_dict = {}
		for link_tree_id, link_tree_detail in link_tree_dict.items():
			out_dict[link_tree_id] = []
			if 'years' in link_tree_detail:
				for year in link_tree_detail['years']:
					for group_id in link_tree_detail['years'][year]['groups']:
						if group_id not in out_dict[link_tree_id]:
							out_dict[link_tree_id].append(group_id)
			else:
				for group_id in link_tree_dict['groups']:
					if group_id not in out_dict[link_tree_id]:
						out_dict[link_tree_id].append(group_id)

		with open(os.path.join(store_path, 'linktree_groups.json'), 'w') as fp:
			json.dump(out_dict, fp, indent=2)

	return out_dict

def compile_linktree_group_years_map(store_path=config_path_, force_fetch=False):
	if os.path.isfile(os.path.join(store_path, 'linktree_years_groups.json')) and not force_fetch:
		out_dict = json.load(open(os.path.join(store_path, 'linktree_years_groups.json'), 'r'))
	else:
		link_tree_dict = compile_non_group_variables_map(store_path, force_fetch)
		out_dict = {}
		for link_tree_id, link_tree_detail in link_tree_dict.items():
			out_dict[link_tree_id] = {}
			out_dict[link_tree_id]['years'] = []
			out_dict[link_tree_id]['groups'] = {}
			if 'years' in link_tree_detail:
				for year in link_tree_detail['years']:
					out_dict[link_tree_id]['years'].append(year)
					for group_id in link_tree_detail['years'][year]['groups']:
						if group_id not in out_dict[link_tree_id]['groups']:
							out_dict[link_tree_id]['groups'][group_id] = []
						out_dict[link_tree_id]['groups'][group_id].append(year)
			else:
				for group_id in link_tree_detail['groups']:
					if group_id not in out_dict[link_tree_id]['groups']:
						out_dict[link_tree_id]['groups'][group_id] = []
					out_dict[link_tree_id]['groups'][group_id].append(year)

		with open(os.path.join(store_path, 'linktree_years_groups.json'), 'w') as fp:
			json.dump(out_dict, fp, indent=2)

	return out_dict

# wrapper functions for finding available options


compile_link_tree_based_map()
fetch_linktree_config_cache('group_variables')
compile_linktree_group_map()
compile_linktree_group_years_map()
	