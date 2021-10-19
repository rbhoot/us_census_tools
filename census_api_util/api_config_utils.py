from api_config_fetcher import *

module_dir_ = os.path.dirname(__file__)
module_parentdir_ = os.path.join(module_dir_, '..')
config_path_ = os.path.join(module_dir_, 'config_files')
sys.path.append(os.path.join(module_dir_, '..'))

FLAGS = flags.FLAGS

# get column list
def get_column_list_all(link_tree, group_id, config_path=config_path_, store_path='~/acs_tables/'):
	group_id = group_id.upper()

	link_tree_dict = compile_linktree_group_years_map()
	if link_tree not in link_tree_dict:
		return []
	if group_id not in link_tree_dict[link_tree]['groups']:
		return []
	
	ret_dict = {}
	ret_dict['all'] = []
	if len(link_tree_dict[link_tree]['groups'][group_id]) > 0:
		for year in link_tree_dict[link_tree]['groups'][group_id]:
			cache_file = os.path.join(config_path, 'api_cache', link_tree, str(year), group_id+'.json')
			if os.path.isfile(cache_file):
				group_variables_dict = json.load(open(cache_file, 'r'))
			else:
				temp_url = generate_url_group_variables(link_tree, group_id, year)
				group_variables_dict = request_url_json(temp_url)

			if 'http_err_code' not in group_variables_dict:
				if 'variables' not in group_variables_dict:
					print(cache_file, 'contains invalid data')
				else:
					ret_dict[year] = []
					for variable_id, variable_dict in group_variables_dict['variables'].items():
						if not variable_id.endswith('A'):
							ret_dict[year].append(variable_dict['label'])
							if variable_dict['label'] not in ret_dict['all']:
								ret_dict['all'].append(variable_dict['label'])

	else:
		cache_file = os.path.join(config_path, 'api_cache', link_tree, group_id+'.json')
		if os.path.isfile(cache_file):
			group_variables_dict = json.load(open(cache_file, 'r'))
		else:
			temp_url = generate_url_group_variables(link_tree, group_id)
			group_variables_dict = request_url_json(temp_url)

		if 'http_err_code' not in group_variables_dict:
			if 'variables' not in group_variables_dict:
				print(cache_file, 'contains invalid data')
			else:
				for variable_id, variable_dict in group_variables_dict['variables'].items():
					if not variable_id.endswith('A'):
						if variable_dict['label'] not in ret_dict['all']:
							ret_dict['all'].append(variable_dict['label'])

	store_path = os.path.expanduser(store_path)
	store_path = os.path.join(store_path, group_id)
	if not os.path.exists(store_path):
		os.makedirs(store_path, exist_ok=True)
	
	with open(os.path.join(store_path, 'yearwise_columns.json'), 'w') as fp:
		json.dump(ret_dict, fp, indent=2)
	with open(os.path.join(store_path, 'all_columns.json'), 'w') as fp:
		json.dump(ret_dict['all'], fp, indent=2)
	
	return ret_dict

def get_column_list_years(link_tree, group_id, year_list, config_path=config_path_, store_path='~/acs_tables/'):
	group_id = group_id.upper()
	columns_dict = get_column_list_all(link_tree, group_id, config_path, store_path)
	ret_dict = {}
	ret_dict['all'] = []
	for year in columns_dict:
		if year != 'all' and int(year) in year_list:
			ret_dict[int(year)] = columns_dict[year]

	# print if years missing
	missing_years = list(set(year_list)-set(list(ret_dict.keys())))
	if len(missing_years) > 0:
		print('Warning: data missing for ', missing_years)
	
	# compile all section
	for year in columns_dict:
		for column_name in columns_dict[year]:
			if column_name not in ret_dict['all']:
				ret_dict['all'].append(column_name)
	
	store_path = os.path.expanduser(store_path)
	store_path = os.path.join(store_path, group_id)
	if not os.path.exists(store_path):
		os.makedirs(store_path, exist_ok=True)
	
	with open(os.path.join(store_path, 'year_list_columns.json'), 'w') as fp:
		json.dump(ret_dict, fp, indent=2)

	return ret_dict


# get variables from yearwise column list (with_annotations=True)

# available_linktree

# available years (linktree, group)

# available groups

# available geos (linktree, year)

get_column_list_all('acs/acs5/subject', 's1810')
# get_column_list_years('acs/acs5/subject', 's1810', list(range(2010, 2019)))