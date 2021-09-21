import json
import os
import requests
import ast
import copy
import sys

module_dir_ = os.path.dirname(__file__)
sys.path.append(os.path.join(module_dir_, '..'))

from common_utils.common_util import getTokensListFromZip, columnsFromZipFile, tokenInListIgnoreCase
from dc_api_tools.dc_utils import fetch_dcid_properties_enums
from spec_validator.acs_spec_validator import findColumnsWithNoProperties, findMissingTokens


spec_dir = '../spec_dir/'
expected_populations = ['Person', 'Household']
expected_pvs = ['age', 'gender', 'race', 'nativity', 'citizenship', 'maritalStatus', 'residentStatus', 'educationalAttainment', 'income', 'povertyStatus', 'occupancyTenure']

# TODO might have to change this if invocation from elsewhere is to be allowed
def get_spec_list(spec_dir='../spec_dir/'):
	spec_dir = os.path.expanduser(spec_dir)

	spec_list = []
	# read all json files
	for filename in os.listdir(spec_dir):
		if filename.endswith('.json'):
			with open(spec_dir+filename, 'r') as fp:
				spec_list.append(json.load(fp))
	return spec_list

# print list of available pvs
def get_available_pvs_spec(all_specs):
	ret_list = []
	# TODO store the source file name
	for cur_spec in all_specs:
		ret_list.extend(cur_spec['pvs'].keys())
	return list(set(ret_list))

# print(get_available_pvs(spec_list))

# TODO print available pvs from dc

# create megaspec
def create_combined_spec(all_specs):
	out_spec = {}
	out_spec['populationType'] = {}
	out_spec['measurement'] = {}
	out_spec['enumSpecializations'] = {}
	out_spec['pvs'] = {}
	out_spec['inferredSpec'] = {}
	out_spec['universePVs'] = []
	out_spec['ignoreColumns'] = []

	for cur_spec in all_specs:
		out_spec['populationType']['_DEFAULT'] = "Person XXXXX"
		if 'populationType' in cur_spec:
			for population_token in cur_spec['populationType']:
				if not population_token.startswith('_'):
					if population_token not in out_spec['populationType']:
						out_spec['populationType'][population_token] = cur_spec['populationType'][population_token]
					elif out_spec['populationType'][population_token] != cur_spec['populationType'][population_token]:
						print("Error:", population_token, "already assigned to population", out_spec['populationType'][population_token], "new value:", cur_spec['populationType'][population_token])
		
		out_spec['measurement']['_DEFAULT'] = {
		            "measuredProperty": "count XXXXX",
		            "statType": "measuredValue"
		        }
		# TODO this might have potential conflicts that need to be merged
		if 'measurement' in cur_spec:
			for measurement_token in cur_spec['measurement']:
				if not measurement_token.startswith('_'):
					if measurement_token not in out_spec['measurement']:
						out_spec['measurement'][measurement_token] = {}
						out_spec['measurement'][measurement_token].update(cur_spec['measurement'][measurement_token])
					elif out_spec['measurement'][measurement_token] != cur_spec['measurement'][measurement_token]:
						print("Error:", measurement_token, "already assigned to measurement", out_spec['measurement'][measurement_token], "new value:", cur_spec['measurement'][measurement_token])

		if 'enumSpecializations' in cur_spec:
			for enum_token in cur_spec['enumSpecializations']:
				if not enum_token.startswith('_'):
					if enum_token not in out_spec['enumSpecializations']:
						out_spec['enumSpecializations'][enum_token] = cur_spec['enumSpecializations'][enum_token]
					elif out_spec['enumSpecializations'][enum_token] != cur_spec['enumSpecializations'][enum_token]:
						print("Error:", enum_token, "already assigned to enumSpecialization", out_spec['enumSpecializations'][enum_token], "new value:", cur_spec['enumSpecializations'][enum_token])
		
		for property_name in cur_spec['pvs']:
			if property_name not in out_spec['pvs']:
				out_spec['pvs'][property_name] = {}
			for property_token in cur_spec['pvs'][property_name]:
				if property_token not in out_spec['pvs'][property_name]:
					out_spec['pvs'][property_name][property_token] = cur_spec['pvs'][property_name][property_token]
				elif out_spec['pvs'][property_name][property_token] != cur_spec['pvs'][property_name][property_token]:
					print("Error:", property_token, "already assigned to pv", out_spec['pvs'][property_name][property_token], "new value:", cur_spec['pvs'][property_name][property_token])

		# TODO this might have potential conflicts that need to be merged
		if 'inferredSpec' in cur_spec:
			for property_name in cur_spec['inferredSpec']:
				if property_name not in out_spec['inferredSpec']:
					out_spec['inferredSpec'][property_name] = {}
					out_spec['inferredSpec'][property_name].update(cur_spec['inferredSpec'][property_name])
				else:
					cur_spec['inferredSpec'][property_name] = {}
					for dependent_prop in cur_spec['inferredSpec'][property_name]:
						if dependent_prop not in out_spec['inferredSpec'][property_name]:
							out_spec['inferredSpec'][property_name][dependent_prop] = cur_spec['inferredSpec'][property_name][dependent_prop]
						elif out_spec['inferredSpec'][property_name][dependent_prop] != cur_spec['inferredSpec'][property_name][dependent_prop]:
							print("Error:", dependent_prop, "already assigned to", property_name, out_spec['inferredSpec'][property_name][dependent_prop], "new value:", cur_spec['inferredSpec'][property_name][dependent_prop])
		
		# add universePVs
		if 'universePVs' in cur_spec:
			for cur_universe in cur_spec['universePVs']:
				if cur_universe not in out_spec['universePVs']:
					out_spec['universePVs'].append(cur_universe)

		if 'ignoreColumns' in cur_spec:
			for column_name in cur_spec['ignoreColumns']:
				if column_name not in out_spec['ignoreColumns']:
					out_spec['ignoreColumns'].append(column_name)

	with open('universal_spec.json', 'w') as fp:
		json.dump(out_spec, fp, indent=2)

	return out_spec



# go through megaspec creating output and discarded spec
def create_new_spec(zip_path, universal_spec, expected_populations=['Person'], expected_pvs=[]):
	zip_path = os.path.expanduser(zip_path)

	# read zip file for tokens
	all_tokens = getTokensListFromZip(zip_path)
	all_columns = columnsFromZipFile(zip_path)

	out_spec = {}
	# assign expected_population[0] to default if present
	out_spec['populationType'] = {}
	out_spec['measurement'] = {}
	out_spec['enumSpecializations'] = {}
	out_spec['pvs'] = {}
	out_spec['inferredSpec'] = {}
	out_spec['universePVs'] = []
	out_spec['denominators'] = {}
	out_spec['ignoreColumns XXXXX'] = []
	out_spec['ignoreTokens'] = []

	discarded_spec = {}
	discarded_spec['populationType'] = {}
	discarded_spec['measurement'] = {}
	discarded_spec['enumSpecializations'] = {}
	discarded_spec['pvs'] = {}
	discarded_spec['inferredSpec'] = {}
	discarded_spec['universePVs'] = []
	discarded_spec['denominators'] = {}
	discarded_spec['ignoreColumns'] = []
	discarded_spec['ignoreTokens'] = []

	for population_token in universal_spec['populationType']:
		if  population_token.startswith('_'):
			out_spec['populationType'][population_token] = universal_spec['populationType'][population_token]
		elif tokenInListIgnoreCase(population_token, all_tokens):
			out_spec['populationType'][population_token] = universal_spec['populationType'][population_token]
		else:
			discarded_spec['populationType'][population_token] = universal_spec['populationType'][population_token]
	
	out_spec['populationType'] = {'_DEFAULT': expected_populations[0]}

	for measurement_token in universal_spec['measurement']:
		if  measurement_token.startswith('_'):
			out_spec['measurement'][measurement_token] = universal_spec['measurement'][measurement_token]
		elif tokenInListIgnoreCase(measurement_token, all_tokens):
			out_spec['measurement'][measurement_token] = universal_spec['measurement'][measurement_token]
		elif measurement_token in all_columns:
			out_spec['measurement'][measurement_token] = universal_spec['measurement'][measurement_token]
		else:
			discarded_spec['measurement'][measurement_token] = universal_spec['measurement'][measurement_token]

	for enum_token in universal_spec['enumSpecializations']:
		if tokenInListIgnoreCase(enum_token, all_tokens):
			out_spec['enumSpecializations'][enum_token] = universal_spec['enumSpecializations'][enum_token]
		else:
			discarded_spec['enumSpecializations'][enum_token] = universal_spec['enumSpecializations'][enum_token]

	for prop in universal_spec['pvs']:
		for property_token in universal_spec['pvs'][prop]:
			if tokenInListIgnoreCase(property_token, all_tokens):
				if prop not in out_spec['pvs']:
					out_spec['pvs'][prop] = {}
				out_spec['pvs'][prop][property_token] = universal_spec['pvs'][prop][property_token]
			else:
				if prop not in discarded_spec['pvs']:
					discarded_spec['pvs'][prop] = {}
				discarded_spec['pvs'][prop][property_token] = universal_spec['pvs'][prop][property_token]

	for prop in universal_spec['inferredSpec']:
		if prop in out_spec['pvs']:
			out_spec['inferredSpec'].update({prop:universal_spec['inferredSpec'][prop]})
		else:
			discarded_spec['inferredSpec'].update({prop:universal_spec['inferredSpec'][prop]})

	for cur_universe in universal_spec['universePVs']:
		population_flag = False
		for population_token in out_spec['populationType']:
			if out_spec['populationType'][population_token] == cur_universe['populationType']:
				population_flag = True
		
		property_flag = True
		for property_name in cur_universe['constraintProperties']:
			if property_name not in out_spec['pvs']:
				property_flag = False

		if property_flag and population_flag:
			out_spec['universePVs'].append(cur_universe)

	# ignoreColumns
	for token_name in universal_spec['ignoreColumns']:
		if tokenInListIgnoreCase(token_name, all_tokens) or token_name in all_columns:
			if token_name not in out_spec['ignoreColumns XXXXX']:
				out_spec['ignoreColumns XXXXX'].append(token_name)


	# TODO denominators, allow column checks


	dc_props = {}
	for population_dcid in expected_populations:
		dc_props[population_dcid] = fetch_dcid_properties_enums(population_dcid)

	# add missing properties from expected properties
	for property_name in expected_pvs:
		if property_name not in out_spec['pvs']:
			out_spec['pvs'][property_name] = {}
			# fetch values from dc if present
		for population_name in dc_props:
			if property_name in dc_props[population_name]:
				if dc_props[population_name][property_name]:
					for i, enum_value in enumerate(dc_props[population_name][property_name]):
						out_spec['pvs'][property_name]['XXXXX'+str(i)] = enum_value
			# TODO guess token if possible

	# TODO householder race also appears because of race related tokens

	# print columns with no pv assignment
	columns_missing_pv = findColumnsWithNoProperties(all_columns, out_spec)
	columns_missing_pv = list(set(columns_missing_pv))
	# print('---------------------')
	# print('columns missing pv')
	# print('---------------------')
	# print(columns_missing_pv)

	# missing tokens
	print('---------------------')
	print('missing tokens')
	print('---------------------')
	missing_tokens = findMissingTokens(all_tokens, out_spec)
	print(missing_tokens)

	# ?TODO check properties appearing in both specs 

	# write to output files
	with open('generated_spec.json', 'w') as fp:
		json.dump(out_spec, fp, indent=2)
	with open('discarded_spec_parts.json', 'w') as fp:
		json.dump(discarded_spec, fp, indent=2)

	# write missing reports to file
	with open('missing_report.json', 'w') as fp:
		json.dump({'columns_missing_pv':columns_missing_pv, 'missing_tokens':missing_tokens}, fp, indent=2)

	return out_spec


