from collections import OrderedDict
import logging
import requests
import json
from absl import app
from absl import flags
import sys
import os
import time
import random
from download_utils import download_url_list_iterations
from status_file_utils import get_pending_or_fail_url_list

module_dir_ = os.path.dirname(__file__)
module_parentdir_ = os.path.join(module_dir_, '..')
config_path_ = os.path.join(module_dir_, 'config_files')
sys.path.append(os.path.join(module_dir_, '..'))

from common_utils.requests_wrappers import request_url_json

FLAGS = flags.FLAGS

def save_resp_json(resp, store_path):
    resp_data = resp.json()
    logging.info('Writing downloaded data to file: %s', store_path)
    json.dump(resp_data, open(store_path, 'w'), indent = 2)

def _generate_url_prefix(dataset, year=None):
  if year:
    return f'https://api.census.gov/data/{year}/{dataset}/'
  else:
    return f'https://api.census.gov/data/{dataset}/'


def generate_url_geography(dataset, year=None):
  return _generate_url_prefix(dataset, year) + 'geography.json'


def generate_url_groups(dataset, year=None):
  return _generate_url_prefix(dataset, year) + 'groups.json'


def generate_url_variables(dataset, year=None):
  return _generate_url_prefix(dataset, year) + 'variables.json'


def generate_url_tags(dataset, year=None):
  return _generate_url_prefix(dataset, year) + 'tags.json'


def generate_url_group_variables(dataset, group_id, year=None):
  return _generate_url_prefix(dataset, year) + f'groups/{group_id}.json'


def fetch_dataset_config(store_path=config_path_, force_fetch=False):
  store_path = os.path.expanduser(store_path)
  if not os.path.exists(store_path):
    os.makedirs(store_path, exist_ok=True)
  if force_fetch or not os.path.isfile(
      os.path.join(store_path, 'dataset_list.json')):
    datasets = request_url_json('https://api.census.gov/data.json')
    if 'http_err_code' not in datasets:
      with open(os.path.join(store_path, 'dataset_list.json'), 'w') as fp:
        json.dump(datasets, fp, indent=2)
  else:
    datasets = json.load(
        open(os.path.join(store_path, 'dataset_list.json'), 'r'))
  return datasets


def compile_year_map(store_path=config_path_, force_fetch=False):

  if os.path.isfile(os.path.join(store_path, 'dataset_year.json')):
    dataset_dict = json.load(
        open(os.path.join(store_path, 'dataset_year.json'), 'r'))
  else:
    datasets = fetch_dataset_config(store_path, force_fetch)
    dataset_dict = {}
    error_dict = {}
    for dataset_dict in datasets['dataset']:
      dataset = '/'.join(dataset_dict['c_dataset'])
      if dataset_dict['c_isAvailable']:
        if dataset not in dataset_dict:
          dataset_dict[dataset] = {}
          dataset_dict[dataset]['years'] = {}

        identifier = dataset_dict['identifier']
        identifier = identifier[identifier.rfind('/') + 1:]

        if 'c_vintage' in dataset_dict:
          year = dataset_dict['c_vintage']
          dataset_dict[dataset]['years'][year] = {}
          dataset_dict[dataset]['years'][year]['title'] = dataset_dict[
              'title']
          dataset_dict[dataset]['years'][year]['identifier'] = identifier

        elif 'c_isTimeseries' in dataset_dict and dataset_dict['c_isTimeseries']:
          year = None

          if 'title' not in dataset_dict[dataset]:
            dataset_dict[dataset]['title'] = dataset_dict['title']
          elif dataset_dict[dataset]['title'] != dataset_dict['title']:
            if 'timeseries_multiple_titles' not in error_dict:
              error_dict['timeseries_multiple_titles'] = []
            error_dict['timeseries_multiple_titles'].append(dataset)
            print(dataset, 'found multiple title')

          if 'identifier' not in dataset_dict[dataset]:
            dataset_dict[dataset]['identifier'] = identifier
          elif dataset_dict[dataset]['identifier'] != identifier:
            if 'timeseries_multiple_identifiers' not in error_dict:
              error_dict['timeseries_multiple_identifiers'] = []
            error_dict['timeseries_multiple_identifiers'].append(dataset)
            print(dataset, 'found multiple identifiers')
        else:
          year = None
          if 'dataset_unkown_type' not in error_dict:
            error_dict['dataset_unkown_type'] = []
          error_dict['dataset_unkown_type'].append(dataset)
          print('/'.join(dataset_dict['c_dataset']),
                'year not available and not timeseries')

        if dataset_dict['distribution'][0]['accessURL'] != _generate_url_prefix(
            dataset, year)[:-1]:
          if 'url_mismatch' not in error_dict:
            error_dict['url_mismatch'] = []
          error_dict['url_mismatch'].append({
              'expected': _generate_url_prefix(dataset, year)[:-1],
              'actual': dataset_dict['distribution'][0]['accessURL']
          })
          print(dataset, 'accessURL unexpected')

        if dataset_dict['c_geographyLink'] != generate_url_geography(
            dataset, year):
          if 'url_mismatch' not in error_dict:
            error_dict['url_mismatch'] = []
          error_dict['url_mismatch'].append({
              'expected': generate_url_geography(dataset, year),
              'actual': dataset_dict['c_geographyLink']
          })
          print(dataset, 'c_geographyLink unexpected')

        if dataset_dict['c_groupsLink'] != generate_url_groups(dataset, year):
          if 'url_mismatch' not in error_dict:
            error_dict['url_mismatch'] = []
          error_dict['url_mismatch'].append({
              'expected': generate_url_groups(dataset, year),
              'actual': dataset_dict['c_groupsLink']
          })
          print(dataset, 'c_groupsLink unexpected')

        if dataset_dict['c_variablesLink'] != generate_url_variables(
            dataset, year):
          if 'url_mismatch' not in error_dict:
            error_dict['url_mismatch'] = []
          error_dict['url_mismatch'].append({
              'expected': generate_url_variables(dataset, year),
              'actual': dataset_dict['c_variablesLink']
          })
          print(dataset, 'c_variablesLink unexpected')

        if 'c_tagsLink' in dataset_dict:
          if dataset_dict['c_tagsLink'] != generate_url_tags(dataset, year):
            if 'url_mismatch' not in error_dict:
              error_dict['url_mismatch'] = []
            error_dict['url_mismatch'].append({
                'expected': generate_url_tags(dataset, year),
                'actual': dataset_dict['c_tagsLink']
            })
            print(dataset, 'c_tagsLink unexpected')

        if len(dataset_dict['distribution']) > 1:
          if 'dataset_multiple_distribution' not in error_dict:
            error_dict['dataset_multiple_distribution'] = []
          error_dict['dataset_multiple_distribution'].append(dataset)
          print(dataset, 'has multiple distribution')

        if 'c_tagsLink' not in dataset_dict:
          if 'missing_tags' not in error_dict:
            error_dict['missing_tags'] = []
          error_dict['missing_tags'].append(dataset)
          print(dataset, 'c_tagsLink not present')
      else:
        if 'unavailable_datasets' not in error_dict:
          error_dict['unavailable_datasets'] = []
        error_dict['unavailable_datasets'].append(dataset)
        print(dataset, 'not available')

    with open(os.path.join(store_path, 'dataset_year.json'), 'w') as fp:
      json.dump(dataset_dict, fp, indent=2)
    if error_dict:
      with open(os.path.join(store_path, 'errors_dataset_year.json'),
                'w') as fp:
        json.dump(error_dict, fp, indent=2)

  return dataset_dict


def fetch_dataset_config_cache(param,
                                store_path=config_path_,
                                force_fetch=False):
  if param not in ['groups', 'geography', 'variables', 'group_variables']:
    error_dict = {'invalid_param': [param]}
    with open(
        os.path.join(store_path, f'errors_dataset_{param}_download.json'),
        'w') as fp:
      json.dump(error_dict, fp, indent=2)
    return

  store_path = os.path.abspath(store_path)

  dataset_dict = compile_year_map(store_path, force_fetch)
  if param == 'group_variables':
    dataset_dict = compile_non_group_variables_map(store_path, force_fetch)
  error_dict = {}
  url_list = []

  cache_path = os.path.join(store_path, 'api_cache')
  if not os.path.exists(cache_path):
    os.makedirs(cache_path, exist_ok=True)

  status_file = os.path.join(cache_path, f'{param}_cache_status.json')

  for dataset in dataset_dict:
    if 'years' in dataset_dict[dataset]:
      for year in dataset_dict[dataset]['years']:
        if param == 'groups':
          temp_url = generate_url_groups(dataset, year)
        elif param == 'geography':
          temp_url = generate_url_geography(dataset, year)
        elif param == 'variables':
          temp_url = generate_url_variables(dataset, year)
        else:
          temp_url = None

        file_path = os.path.join(cache_path, dataset, str(year))
        file_name = os.path.join(file_path, f'{param}.json')
        if not os.path.exists(file_path):
          os.makedirs(file_path, exist_ok=True)

        if temp_url:
          temp_dict = {}
          temp_dict['url'] = temp_url
          temp_dict['output_path'] = file_name
          temp_dict['status'] = 'pending'
          temp_dict['force_fetch'] = force_fetch
          url_list.append(temp_dict)

        if param == 'group_variables':
          for group_id in dataset_dict[dataset]['years'][year]['groups']:
            temp_url = generate_url_group_variables(dataset, group_id, year)
            file_name = os.path.join(file_path, group_id + '.json')
            if temp_url not in url_list:
              temp_dict = {}
              temp_dict['url'] = temp_url
              temp_dict['output_path'] = file_name
              temp_dict['status'] = 'pending'
              temp_dict['force_fetch'] = force_fetch
              url_list.append(temp_dict)
    else:
      if param == 'groups':
        temp_url = generate_url_groups(dataset)
      elif param == 'geography':
        temp_url = generate_url_geography(dataset)
      elif param == 'variables':
        temp_url = generate_url_variables(dataset)
      else:
        temp_url = None

      file_path = os.path.join(cache_path, dataset)
      file_name = os.path.join(file_path, f'{param}.json')
      if not os.path.exists(file_path):
        os.makedirs(file_path, exist_ok=True)
      if temp_url and temp_url not in url_list:
        temp_dict = {}
        temp_dict['url'] = temp_url
        temp_dict['store_path'] = file_name
        temp_dict['status'] = 'pending'
        temp_dict['force_fetch'] = force_fetch
        url_list.append(temp_dict)
      if param == 'group_variables':
        for group_id in dataset_dict[dataset]['groups']:
          temp_url = generate_url_group_variables(dataset, group_id)
          file_name = os.path.join(file_path, group_id + '.json')
          if temp_url not in url_list:
            temp_dict = {}
            temp_dict['url'] = temp_url
            temp_dict['store_path'] = file_name
            temp_dict['status'] = 'pending'
            temp_dict['force_fetch'] = force_fetch
            url_list.append(temp_dict)

  failed_urls_ctr = download_url_list_iterations(url_list, None, '', save_resp_json, cache_path)

  # url_list = url_list_check_downloaded(url_list, force_fetch)

  # status_file = os.path.join(cache_path, f'{param}_cache_status.json')

  # with open(status_file, 'w') as fp:
  #   json.dump(url_list, fp, indent=2)

  # print(len(url_list))

  # error_dict = dowload_url_list_parallel(
  #     status_file, chunk_size=50, chunk_delay_s=0.8)

  if error_dict:
    with open(
        os.path.join(store_path, f'errors_dataset_{param}_download.json'),
        'w') as fp:
      json.dump(get_pending_or_fail_url_list(url_list), fp, indent=2)


def compile_groups_map(store_path=config_path_, force_fetch=False):
  if os.path.isfile(os.path.join(store_path,
                                 'dataset_groups.json')) and not force_fetch:
    dataset_dict = json.load(
        open(os.path.join(store_path, 'dataset_groups.json'), 'r'))
  else:
    dataset_dict = compile_year_map(store_path, force_fetch)
    error_dict = {}
    fetch_dataset_config_cache('groups', store_path, force_fetch)
    cache_path = os.path.join(store_path, 'api_cache')
    for dataset in dataset_dict:
      if 'years' in dataset_dict[dataset]:
        for year in dataset_dict[dataset]['years']:
          cache_file = os.path.join(cache_path, dataset, str(year),
                                    'groups.json')
          temp_url = generate_url_groups(dataset, year)
          if os.path.isfile(cache_file):
            group_list = json.load(open(cache_file, 'r'))
          else:
            group_list = request_url_json(temp_url)
          if 'http_err_code' not in group_list:
            if len(group_list) != 1:
              if 'groups_extra_keys' not in error_dict:
                error_dict['groups_extra_keys'] = []
              error_dict['groups_extra_keys'].append(temp_url)
              print(temp_url, 'has unexpected number of keys ')
            group_list = group_list['groups']
            dataset_dict[dataset]['years'][year]['groups'] = {}
            for cur_group in group_list:
              dataset_dict[dataset]['years'][year]['groups'][
                  cur_group['name']] = {}
              dataset_dict[dataset]['years'][year]['groups'][
                  cur_group['name']]['title'] = cur_group['description']
              # check only 3 key values
              if len(cur_group) != 3:
                if 'groups_extra_keys' not in error_dict:
                  error_dict['groups_extra_keys'] = []
                error_dict['groups_extra_keys'].append(temp_url)
                print(temp_url, 'has unexpected number of keys ')
              # check variables url
              if cur_group['variables'] != generate_url_group_variables(
                  dataset, cur_group['name'], year):
                if 'url_mismatch' not in error_dict:
                  error_dict['url_mismatch'] = []
                error_dict['url_mismatch'].append({
                    'expected':
                        generate_url_group_variables(dataset,
                                                     cur_group['name'], year),
                    'actual':
                        cur_group['variables']
                })
                print(dataset, 'group_variablesLink unexpected')

      else:
        cache_file = os.path.join(cache_path, dataset, 'groups.json')
        temp_url = generate_url_groups(dataset)
        if os.path.isfile(cache_file):
          group_list = json.load(open(cache_file, 'r'))
        else:
          group_list = request_url_json(temp_url)
        if 'http_err_code' not in group_list:
          if len(group_list) != 1:
            if 'groups_extra_keys' not in error_dict:
              error_dict['groups_extra_keys'] = []
            error_dict['groups_extra_keys'].append(temp_url)
            print(temp_url, 'not available')
          group_list = group_list['groups']
          dataset_dict[dataset]['groups'] = {}
          for cur_group in group_list:
            dataset_dict[dataset]['groups'][cur_group['name']] = {}
            dataset_dict[dataset]['groups'][
                cur_group['name']]['title'] = cur_group['description']
            # check only 3 key values
            if len(cur_group) != 3:
              if 'groups_extra_keys' not in error_dict:
                error_dict['groups_extra_keys'] = []
              error_dict['groups_extra_keys'].append(temp_url)
              print(temp_url, 'has unexpected number of keys ')
            # check variables url
            if cur_group['variables'] != generate_url_group_variables(
                dataset, cur_group['name']):
              if 'url_mismatch' not in error_dict:
                error_dict['url_mismatch'] = []
              error_dict['url_mismatch'].append({
                  'expected':
                      generate_url_group_variables(dataset,
                                                   cur_group['name']),
                  'actual':
                      cur_group['variables']
              })
              print(dataset, 'group_variablesLink unexpected')

    with open(os.path.join(store_path, 'dataset_groups.json'), 'w') as fp:
      json.dump(dataset_dict, fp, indent=2)
    if error_dict:
      with open(os.path.join(store_path, 'errors_dataset_groups.json'),
                'w') as fp:
        json.dump(error_dict, fp, indent=2)

  return dataset_dict


def compile_geography_map(store_path=config_path_, force_fetch=False):
  if os.path.isfile(os.path.join(store_path,
                                 'dataset_geography.json')) and not force_fetch:
    dataset_dict = json.load(
        open(os.path.join(store_path, 'dataset_geography.json'), 'r'))
  else:
    dataset_dict = compile_groups_map(store_path, force_fetch)
    error_dict = {}
    fetch_dataset_config_cache('geography', store_path, force_fetch)
    cache_path = os.path.join(store_path, 'api_cache')
    for dataset in dataset_dict:
      if 'years' in dataset_dict[dataset]:
        for year in dataset_dict[dataset]['years']:
          cache_file = os.path.join(cache_path, dataset, str(year),
                                    'geography.json')
          temp_url = generate_url_geography(dataset, year)
          if os.path.isfile(cache_file):
            geo_list = json.load(open(cache_file, 'r'))
          else:
            geo_list = request_url_json(temp_url)
          if 'http_err_code' not in geo_list:
            if len(geo_list) != 1:
              if 'groups_extra_keys' not in error_dict:
                error_dict['groups_extra_keys'] = []
              error_dict['groups_extra_keys'].append(temp_url)
              print(temp_url, 'has unexpected number of keys ')
            if 'fips' in geo_list:
              geo_list = geo_list['fips']
              dataset_dict[dataset]['years'][year]['geos'] = OrderedDict()
              geo_config = dataset_dict[dataset]['years'][year]['geos']
              geo_config['required_geos'] = []
              geo_config['summary_levels'] = OrderedDict()
              for cur_geo in geo_list:
                if 'geoLevelDisplay' in cur_geo and 'geoLevelId' in cur_geo and cur_geo['geoLevelDisplay'] != cur_geo['geoLevelId']:
                  if 'geo_multiple_id' not in error_dict:
                    error_dict['geo_multiple_id'] = []
                  error_dict['geo_multiple_id'].append(temp_url + ' ' + cur_geo['name'])
                  print(cur_geo['name'], 'has multiple geoId ')
                # check only 3 key values
                if len(cur_geo) > 7:
                  if 'groups_extra_keys' not in error_dict:
                    error_dict['groups_extra_keys'] = []
                  error_dict['groups_extra_keys'].append(temp_url)
                  print(temp_url, 'has unexpected number of keys ')

                if 'geoLevelId' in cur_geo:
                  s_level = cur_geo['geoLevelId']
                elif 'geoLevelDisplay' in cur_geo:
                  s_level = cur_geo['geoLevelId']
                else:
                  s_level = cur_geo['name']
                  if 'geo_missing_id' not in error_dict:
                    error_dict['geo_missing_id'] = []
                  error_dict['geo_missing_id'].append(temp_url + ' ' +
                                                      cur_geo['name'])
                  print(cur_geo['name'], 'has no geoId, using name instead.')
                if s_level not in geo_config:
                  geo_config['summary_levels'][s_level] = {}
                  geo_config['summary_levels'][s_level]['str'] = cur_geo['name']
                  if 'requires' in cur_geo:
                    geo_config['summary_levels'][s_level]['geo_filters'] = cur_geo['requires']
                  else:
                    geo_config['summary_levels'][s_level]['geo_filters'] = []
                  if 'wildcard' in cur_geo:
                    geo_config['summary_levels'][s_level]['wildcard'] = cur_geo['wildcard']
                  else:
                    geo_config['summary_levels'][s_level]['wildcard'] = []
                  geo_config['summary_levels'][s_level]['requires'] = []
                  for geo in geo_config['summary_levels'][s_level]['geo_filters']:
                    if geo not in geo_config['summary_levels'][s_level]['wildcard']:
                      geo_config['summary_levels'][s_level]['requires'].append(geo)
                      if geo not in geo_config['required_geos']:
                        geo_config['required_geos'].append(geo)
                else:
                  if 'geo_multiple_id' not in error_dict:
                    error_dict['geo_multiple_id'] = []
                  error_dict['geo_multiple_id'].append(temp_url + ' ' +
                                                        cur_geo['name'])
                  print(cur_geo['name'], 'has multiple geoId ')
            else:
              if 'fips_missing' not in error_dict:
                error_dict['fips_missing'] = []
              error_dict['fips_missing'].append(temp_url)
              print(temp_url, 'fips missing')
      else:
        cache_file = os.path.join(cache_path, dataset, 'geography.json')
        temp_url = generate_url_geography(dataset)
        
        if os.path.isfile(cache_file):
          geo_list = json.load(open(cache_file, 'r'))
        else:
          geo_list = request_url_json(temp_url)
        if 'http_err_code' not in geo_list:
          if len(geo_list) != 1:
            if 'groups_extra_keys' not in error_dict:
              error_dict['groups_extra_keys'] = []
            error_dict['groups_extra_keys'].append(temp_url)
            print(temp_url, 'has unexpected number of keys ')
          if 'fips' in geo_list:
            geo_list = geo_list['fips']
            dataset_dict[dataset]['geos'] = OrderedDict()
            geo_config = dataset_dict[dataset]['geos']
            geo_config['required_geos'] = []
            geo_config['summary_levels'] = OrderedDict()
            for cur_geo in geo_list:
              if 'geoLevelDisplay' in cur_geo and 'geoLevelId' in cur_geo and cur_geo['geoLevelDisplay'] != cur_geo['geoLevelId']:
                if 'geo_multiple_id' not in error_dict:
                  error_dict['geo_multiple_id'] = []
                error_dict['geo_multiple_id'].append(temp_url + ' ' + cur_geo['name'])
                print(cur_geo['name'], 'has multiple geoId ')
              # check only 3 key values
              if len(cur_geo) > 7:
                if 'groups_extra_keys' not in error_dict:
                  error_dict['groups_extra_keys'] = []
                error_dict['groups_extra_keys'].append(temp_url)
                print(temp_url, 'has unexpected number of keys ')

              if 'geoLevelId' in cur_geo:
                s_level = cur_geo['geoLevelId']
              elif 'geoLevelDisplay' in cur_geo:
                s_level = cur_geo['geoLevelId']
              else:
                s_level = cur_geo['name']
                if 'geo_missing_id' not in error_dict:
                  error_dict['geo_missing_id'] = []
                error_dict['geo_missing_id'].append(temp_url + ' ' +
                                                    cur_geo['name'])
                print(cur_geo['name'], 'has no geoId, using name instead.')
              if s_level not in geo_config:
                geo_config['summary_levels'][s_level] = {}
                geo_config['summary_levels'][s_level]['str'] = cur_geo['name']
                if 'requires' in cur_geo:
                  geo_config['summary_levels'][s_level]['geo_filters'] = cur_geo['requires']
                else:
                  geo_config['summary_levels'][s_level]['geo_filters'] = []
                if 'wildcard' in cur_geo:
                  geo_config['summary_levels'][s_level]['wildcard'] = cur_geo['wildcard']
                else:
                  geo_config['summary_levels'][s_level]['wildcard'] = []
                geo_config['summary_levels'][s_level]['requires'] = []
                for geo in geo_config['summary_levels'][s_level]['geo_filters']:
                  if geo not in geo_config['summary_levels'][s_level]['wildcard']:
                    geo_config['summary_levels'][s_level]['requires'].append(geo)
                    if geo not in geo_config['required_geos']:
                      geo_config['required_geos'].append(geo)
              else:
                if 'geo_multiple_id' not in error_dict:
                  error_dict['geo_multiple_id'] = []
                error_dict['geo_multiple_id'].append(temp_url + ' ' +
                                                      cur_geo['name'])
                print(cur_geo['name'], 'has multiple geoId ')
          else:
            if 'fips_missing' not in error_dict:
              error_dict['fips_missing'] = []
            error_dict['fips_missing'].append(temp_url)
            print(temp_url, 'fips missing')

    with open(os.path.join(store_path, 'dataset_geography.json'), 'w') as fp:
      json.dump(dataset_dict, fp, indent=2)
    if error_dict:
      with open(os.path.join(store_path, 'errors_dataset_geography.json'),
                'w') as fp:
        json.dump(error_dict, fp, indent=2)

  return dataset_dict


def compile_non_group_variables_map(store_path=config_path_, force_fetch=False):
  if os.path.isfile(
      os.path.join(store_path,
                   'dataset_non_group_variables.json')) and not force_fetch:
    dataset_dict = json.load(
        open(os.path.join(store_path, 'dataset_non_group_variables.json'), 'r'))
  else:
    dataset_dict = compile_geography_map(store_path, force_fetch)
    error_dict = {}
    fetch_dataset_config_cache('variables', store_path, force_fetch)
    cache_path = os.path.join(store_path, 'api_cache')
    for dataset in dataset_dict:
      if 'years' in dataset_dict[dataset]:
        for year in dataset_dict[dataset]['years']:
          cache_file = os.path.join(cache_path, dataset, str(year),
                                    'variables.json')
          temp_url = generate_url_variables(dataset, year)
          if os.path.isfile(cache_file):
            variable_list = json.load(open(cache_file, 'r'))
          else:
            variable_list = request_url_json(temp_url)
          if 'http_err_code' not in variable_list:
            if len(variable_list) != 1:
              if 'groups_extra_keys' not in error_dict:
                error_dict['groups_extra_keys'] = []
              error_dict['groups_extra_keys'].append(temp_url)
              print(temp_url, 'has unexpected number of keys ')
            if 'variables' in variable_list:
              variable_list = variable_list['variables']
              dataset_dict[dataset]['years'][year]['variables'] = {}
              for cur_variable in variable_list:
                if 'group' not in variable_list[cur_variable] or variable_list[
                    cur_variable]['group'] == 'N/A':
                  dataset_dict[dataset]['years'][year]['variables'][
                      cur_variable] = {}
                  dataset_dict[dataset]['years'][year]['variables'][
                      cur_variable]['label'] = variable_list[cur_variable][
                          'label']
                  if 'concept' in variable_list[cur_variable]:
                    dataset_dict[dataset]['years'][year]['variables'][
                        cur_variable]['concept'] = variable_list[cur_variable][
                            'concept']
                  if 'predicateType' in variable_list[cur_variable]:
                    dataset_dict[dataset]['years'][year]['variables'][
                        cur_variable]['predicateType'] = variable_list[
                            cur_variable]['predicateType']
            else:
              if 'variables_missing' not in error_dict:
                error_dict['variables_missing'] = []
              error_dict['variables_missing'].append(temp_url)
              print(temp_url, 'has no variables section')
      else:
        cache_file = os.path.join(cache_path, dataset, year, 'variables.json')
        temp_url = generate_url_variables(dataset)
        if os.path.isfile(cache_file):
          variable_list = json.load(open(cache_file, 'r'))
        else:
          variable_list = request_url_json(temp_url)
        if 'http_err_code' not in variable_list:
          if len(variable_list) != 1:
            if 'groups_extra_keys' not in error_dict:
              error_dict['groups_extra_keys'] = []
            error_dict['groups_extra_keys'].append(temp_url)
            print(temp_url, 'has unexpected number of keys ')
          if 'variables' in variable_list:
            variable_list = variable_list['variables']
            dataset_dict[dataset]['variables'] = {}
            for cur_variable in variable_list:
              if 'group' not in variable_list[cur_variable] or variable_list[
                  cur_variable]['group'] == 'N/A':
                dataset_dict[dataset]['variables'][cur_variable] = {}
                dataset_dict[dataset]['variables'][cur_variable][
                    'label'] = variable_list[cur_variable]['label']
                if 'concept' in variable_list[cur_variable]:
                  dataset_dict[dataset]['variables'][cur_variable][
                      'concept'] = variable_list[cur_variable]['concept']
                if 'predicateType' in variable_list[cur_variable]:
                  dataset_dict[dataset]['variables'][cur_variable][
                      'predicateType'] = variable_list[cur_variable][
                          'predicateType']
          else:
            if 'variables_missing' not in error_dict:
              error_dict['variables_missing'] = []
            error_dict['variables_missing'].append(temp_url)
            print(temp_url, 'has no variables section')

    with open(
        os.path.join(store_path, 'dataset_non_group_variables.json'),
        'w') as fp:
      json.dump(dataset_dict, fp, indent=2)
    if error_dict:
      with open(
          os.path.join(store_path, 'errors_dataset_non_group_variables.json'),
          'w') as fp:
        json.dump(error_dict, fp, indent=2)

  return dataset_dict


def compile_dataset_based_map(store_path=config_path_, force_fetch=False):
  # compile_year_map(store_path)
  # compile_groups_map(store_path, force_fetch)
  # compile_geography_map(store_path, force_fetch)
  dataset_dict = compile_non_group_variables_map(store_path, force_fetch)
  # dataset_dict = compile_group_variables_map(store_path, force_fetch)

  return dataset_dict


def compile_dataset_group_map(store_path=config_path_, force_fetch=False):
  if os.path.isfile(os.path.join(store_path,
                                 'dataset_groups.json')) and not force_fetch:
    out_dict = json.load(
        open(os.path.join(store_path, 'dataset_groups.json'), 'r'))
  else:
    dataset_dict = compile_non_group_variables_map(store_path, force_fetch)
    out_dict = {}
    for dataset_id, dataset_detail in dataset_dict.items():
      out_dict[dataset_id] = []
      if 'years' in dataset_detail:
        for year in dataset_detail['years']:
          for group_id in dataset_detail['years'][year]['groups']:
            if group_id not in out_dict[dataset_id]:
              out_dict[dataset_id].append(group_id)
      else:
        for group_id in dataset_dict['groups']:
          if group_id not in out_dict[dataset_id]:
            out_dict[dataset_id].append(group_id)

    with open(os.path.join(store_path, 'dataset_groups.json'), 'w') as fp:
      json.dump(out_dict, fp, indent=2)

  return out_dict


def compile_dataset_group_years_map(store_path=config_path_,
                                     force_fetch=False):
  if os.path.isfile(os.path.join(
      store_path, 'dataset_years_groups.json')) and not force_fetch:
    out_dict = json.load(
        open(os.path.join(store_path, 'dataset_years_groups.json'), 'r'))
  else:
    dataset_dict = compile_non_group_variables_map(store_path, force_fetch)
    out_dict = {}
    for dataset_id, dataset_detail in dataset_dict.items():
      out_dict[dataset_id] = {}
      out_dict[dataset_id]['years'] = []
      out_dict[dataset_id]['groups'] = {}
      if 'years' in dataset_detail:
        for year in dataset_detail['years']:
          out_dict[dataset_id]['years'].append(year)
          for group_id in dataset_detail['years'][year]['groups']:
            if group_id not in out_dict[dataset_id]['groups']:
              out_dict[dataset_id]['groups'][group_id] = []
            out_dict[dataset_id]['groups'][group_id].append(year)
      else:
        for group_id in dataset_detail['groups']:
          if group_id not in out_dict[dataset_id]['groups']:
            out_dict[dataset_id]['groups'][group_id] = []
          out_dict[dataset_id]['groups'][group_id].append(year)

    with open(os.path.join(store_path, 'dataset_years_groups.json'),
              'w') as fp:
      json.dump(out_dict, fp, indent=2)

  return out_dict


# wrapper functions for finding available options

compile_dataset_based_map()
fetch_dataset_config_cache('group_variables')
compile_dataset_group_map()
compile_dataset_group_years_map()