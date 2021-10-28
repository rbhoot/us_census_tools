import zipfile
import csv
import io
import pprint
import json
import sys
import os
from absl import app
from absl import flags
import copy 

module_dir_ = os.path.dirname(__file__)
sys.path.append(os.path.join(module_dir_, '..'))

from common_utils.common_util import *

FLAGS = flags.FLAGS

flags.DEFINE_string('validator_output_path', '../output/', 'Path to store the output files')
flags.DEFINE_multi_enum('tests', ['all'], ['all', 'extra_tokens', 'missing_tokens', 'column_no_pv', 'ignore_conflicts', 'enum_specialialisations', 'denominators', 'extra_inferred', 'multiple_measurement', 'multiple_population'], 'List of tests to run')
flags.DEFINE_list('zip_path_list', None, 'List of paths to zip files downloaded from US Census')
flags.DEFINE_string('column_list_path', None, 'Path of json file containing list of all columns')

# finds any extra tokens that appear in the spec as a lookup but not as a part of any of the column names
# requires column list before ignored columns are removed

def find_extra_tokens(columnNameList, specDict, delimiter='!!'):
	retList = []
	# get list of unique tokens across all columns 
	tokenList = get_tokens_list_from_column_list(columnNameList, delimiter)
	
	retList = get_spec_token_list(specDict, delimiter)['token_list']
	
	tokensCopy = retList.copy()
	
	# ignore tokens beginning with an underscore or if token is a column name and appears in columnNameList
	for token in tokensCopy:
		if token.startswith('_'):
			retList.remove(token)
		elif token_in_list_ignore_case(token, tokenList):
			retList.remove(token)
		if delimiter in token:
			if token in columnNameList:
				retList.remove(token)
	return retList


# finds all columns that do not assign any property a value
# assumes columnNameList does not contain columns to be ignored
def find_columns_with_no_properties(columnNameList, specDict, delimiter='!!'):
	retList = []
	for columnName in columnNameList:
		noPropFlag = True
		# get token list of the column
		for token in columnName.split(delimiter):
			for prop in specDict['pvs'].keys():
				if token_in_list_ignore_case(token, specDict['pvs'][prop].keys()):
					# clear the flag when some property gets assigned a value
					noPropFlag = False
		# if the flag has remained set across all properties
		if noPropFlag:
			retList.append(columnName)
	return retList

# returns list of tokens that appear in ignoreColumn as well as a PV
# checks only tokens, ignores long column names
def find_ignore_conflicts(specDict, delimiter='!!'):
	retList = []
	
	newDict = copy.deepcopy(specDict)
	newDict.pop('ignoreColumns', None)
	newDict.pop('ignoreTokens', None)

	specTokens = get_spec_token_list(newDict, delimiter)['token_list']

	if 'ignoreColumns' in specDict:
		for ignoreToken in specDict['ignoreColumns']:
			if ignoreToken in specTokens:
				retList.append(ignoreToken)

	if 'ignoreTokens' in specDict:
		for ignoreToken in specDict['ignoreTokens']:
			if ignoreToken in specTokens:
				retList.append(ignoreToken)

	return retList

# if multiple tokens match same property, they should appear as enumspecialisation
# the token that appears later in the name should be the specialisation of one one encountered before
# assumes columnNameList does not contain columns to be ignored
def find_missing_enum_specialisation(columnNameList, specDict, delimiter='!!'):
	retDict = {}
	for columnName in columnNameList:
		tempDict = {}
		# populate a dictionary containing properties and all the values assigned to it
		for token in columnName.split(delimiter):
			for prop in specDict['pvs'].keys():
				if token_in_list_ignore_case(token, specDict['pvs'][prop].keys()):
					if prop in tempDict:
						tempDict[prop].append(token)
					else:
						tempDict[prop] = [token]
		# check all the columns that have multiple values assigned to a single property
		for prop in tempDict:
			if len(tempDict[prop]) > 1:
				# retDict.append(tempDict[prop])
				for i, propToken in enumerate(reversed(tempDict[prop])):
					j = len(tempDict[prop])-1-i

					tempFlag = True
					# if token appears as a specialisation but it's base doesn't appear before it
					if 'enumSpecializations' in specDict:
						if token_in_list_ignore_case(propToken, specDict['enumSpecializations']):
							tempFlag = False
							if specDict['enumSpecializations'][propToken] not in tempDict[prop][:j]:
								if propToken not in retDict:
									retDict[propToken] = {}
									retDict[propToken]['column'] = [columnName]
									retDict[propToken]['possibleParents'] = tempDict[prop][:j]
								else:
									retDict[propToken]['column'].append(columnName)
									retDict[propToken]['possibleParents'].extend(tempDict[prop][:j])
					# if the token is near the leaf but not used as a specialisation, it potentially has a base value
					if j > 0 and tempFlag:
						if propToken not in retDict:
							retDict[propToken] = {}
							retDict[propToken]['column'] = [columnName]
							retDict[propToken]['possibleParents'] = tempDict[prop][:j]
						else:
							retDict[propToken]['column'].append(columnName)
							retDict[propToken]['possibleParents'].extend(tempDict[prop][:j])
	
	for propToken in retDict:
		retDict[propToken]['possibleParents'] = list(set(retDict[propToken]['possibleParents']))

	return retDict

def find_multiple_measurement(columnNameList, specDict, delimiter = '!!'):
	retList = []
	
	# tokenList = getTokensListFromColumnList(columnNameList, delimiter)
	for columnName in columnNameList:
		if 'measurement' in specDict:
			tempFlag = False
			for token in columnName.split(delimiter):
				if token in specDict['measurement']:
					if tempFlag:
						retList.append(columnName)
					tempFlag = True
	return retList

def find_multiple_population(columnNameList, specDict, delimiter = '!!'):
	retList = []
	
	# tokenList = getTokensListFromColumnList(columnNameList, delimiter)
	for columnName in columnNameList:
		if 'populationType' in specDict:
			tempFlag = False
			for token in columnName.split(delimiter):
				if token in specDict['populationType']:
					if tempFlag and curPopulation != specDict['populationType'][token]:
						retList.append(columnName)
					else:
						curPopulation = specDict['populationType'][token]
					tempFlag = True
	return retList

# check if all the columns that appear as total exist
# assumes columnNameList does not contain columns to be ignored
def find_missing_denominator_total_column(columnNameList, specDict, delimiter = '!!'):
	retList = []
	
	tokenList = get_tokens_list_from_column_list(columnNameList, delimiter)

	if 'denominators' in specDict:
		for totalColumn in specDict['denominators'].keys():
			if delimiter in totalColumn:
				if totalColumn not in columnNameList:
					retList.append(totalColumn)
			elif not token_in_list_ignore_case(totalColumn, tokenList):
				retList.append(totalColumn)
	return retList

def find_missing_denominators(columnNameList, specDict, delimiter = '!!'):
	retList = []
	
	tokenList = get_tokens_list_from_column_list(columnNameList, delimiter)

	if 'denominators' in specDict:
		for totalColumn in specDict['denominators'].keys():
			for curDenominator in specDict['denominators'][totalColumn]:
				if delimiter in curDenominator:
					if curDenominator not in columnNameList:
						retList.append(curDenominator)
				elif not token_in_list_ignore_case(curDenominator, tokenList):
					retList.append(curDenominator)
	return retList

def find_repeating_denominators(columnNameList, specDict, delimiter = '!!'):
	retList = []
	appearedList = []

	tokenList = get_tokens_list_from_column_list(columnNameList, delimiter)

	if 'denominators' in specDict:
		for totalColumn in specDict['denominators'].keys():
			for curDenominator in specDict['denominators'][totalColumn]:
				if token_in_list_ignore_case(curDenominator, appearedList):
					retList.append(curDenominator)
				else:
					appearedList.append(curDenominator)
	return retList
	

# runs all the tests related to tokens and columns and prints relevant output
# requires column list before ignored columns are removed
# raiseWarningsOnly is used mainly in case of yearwise processing, it prevents raising of false errors
def test_column_name_list(columnNameList, specDict, test_list=['all'], raiseWarningsOnly = False, delimiter = '!!'):
	retDict = {}

	# remove ignore columns
	columnNameList = remove_columns_to_be_ignored(columnNameList, specDict, delimiter)
	
	tokenList = get_tokens_list_from_column_list(columnNameList, delimiter)
	
	if 'all' in test_list or 'missing_tokens' in test_list:
		tempList = find_missing_tokens(tokenList, specDict)
		retDict['missing_tokens'] = []
		if len(tempList) > 0:
			print("\nWarning: Following tokens are missing in the spec")
			tempList = list(set(tempList))
			print(json.dumps(tempList, indent=2))
			retDict['missing_tokens'] = tempList
		else:
			print("All tokens present in spec or ignored")
	
	if 'all' in test_list or 'column_no_pv' in test_list:
		tempList = find_columns_with_no_properties(columnNameList, specDict)
		retDict['no_pv_columns'] = []
		if len(tempList) > 0:
			print("\nWarning: Following columns do not have any property assigned")
			tempList = list(set(tempList))
			print(json.dumps(tempList, indent=2))
			retDict['no_pv_columns'] = tempList
		else:
			print("All columns have PV assignment")
	
	if 'all' in test_list or 'ignore_conflicts' in test_list:
		tempList = find_ignore_conflicts(specDict)
		retDict['ignore_conflicts_token'] = []
		if len(tempList) > 0:
			if raiseWarningsOnly:
				print("\nWarning: Following tokens appear in ignore list as well as property value")
			else:
				print("\nError: Following tokens appear in ignore list as well as property value")
			tempList = list(set(tempList))
			print(json.dumps(tempList, indent=2))
			retDict['ignore_conflicts_token'] = tempList
		else:
			print("No conflicting token assigned")
	
	if 'all' in test_list or 'enum_specialialisations' in test_list:
		retDict['enum_specializations_missing'] = {}
		tempDict = find_missing_enum_specialisation(columnNameList, specDict)
		if len(tempDict) > 0:
			print("\nWarning: Following tokens should have an enumSpecialization")
			print(json.dumps(tempDict, indent=2))
			retDict['enum_specializations_missing'] = tempDict
		else:
			print("All tokens in enumSpecialization found")

	if 'all' in test_list or 'denominators' in test_list:
		tempList = find_missing_denominator_total_column(columnNameList, specDict, delimiter)
		retDict['missing_denominator_totals'] = []
		if len(tempList) > 0:
			if raiseWarningsOnly:
				print("\nWarning: Following denominator total columns not found")
			else:
				print("\nError: Following denominator total columns not found")
			tempList = list(set(tempList))
			print(json.dumps(tempList, indent=2))
			retDict['missing_denominator_totals'] = tempList
		else:
			print("All denominator total columns found")

		tempList = find_missing_denominators(columnNameList, specDict, delimiter)
		retDict['missing_denominator'] = []
		if len(tempList) > 0:
			if raiseWarningsOnly:
				print("\nWarning: Following denominator were not found")
			else:
				print("\nError: Following denominator were not found")
			tempList = list(set(tempList))
			print(json.dumps(tempList, indent=2))
			retDict['missing_denominator'] = tempList
		else:
			print("All denominators were found")

		tempList = find_repeating_denominators(columnNameList, specDict, delimiter)
		retDict['repeating_denominator'] = []
		if len(tempList) > 0:
			if raiseWarningsOnly:
				print("\nWarning: Following denominator were repeated")
			else:
				print("\nError: Following denominator were repeated")
			tempList = list(set(tempList))
			print(json.dumps(tempList, indent=2))
			retDict['repeating_denominator'] = tempList
		else:
			print("No denominators were repeated")
	if 'all' in test_list or 'multiple_measurement' in test_list:
		tempList = find_multiple_measurement(columnNameList, specDict, delimiter)
		retDict['multiple_measurement'] = []
		if len(tempList) > 0:
			if raiseWarningsOnly:
				print("\nWarning: Following columns assigned multiple measurements")
			else:
				print("\nError: Following columns assigned multiple measurements")
			tempList = list(set(tempList))
			print(json.dumps(tempList, indent=2))
			retDict['multiple_measurement'] = tempList
		else:
			print("No multiple measurements found")

	if 'all' in test_list or 'multiple_population' in test_list:
		tempList = find_multiple_population(columnNameList, specDict, delimiter)
		retDict['multiple_population'] = []
		if len(tempList) > 0:
			if raiseWarningsOnly:
				print("\nWarning: Following columns assigned multiple population")
			else:
				print("\nError: Following columns assigned multiple population")
			tempList = list(set(tempList))
			print(json.dumps(tempList, indent=2))
			retDict['multiple_population'] = tempList
		else:
			print("No multiple population found")

	return retDict

def find_extra_inferred_properties(specDict):
	retList = []
	if 'inferredSpec' in specDict:
		for property_name in specDict['inferredSpec']:
			if property_name not in specDict['pvs']:
				retList.append(property_name)
	return retList

# calls all methods to check the spec 
def test_spec(columnNameList, specDict, test_list=['all'], delimiter='!!'):
	retDict = {}
	if 'all' in test_list or 'extra_tokens' in test_list:
		tempList = find_extra_tokens(columnNameList, specDict)
		retDict['extra_tokens'] = tempList
		if len(tempList) > 0:
			print("\nError: Following tokens appear in the spec but not in csv")
			tempList = list(set(tempList))
			print(json.dumps(tempList, indent=2))
		else:
			print("No extra tokens in spec")

		tempList = get_spec_token_list(specDict, delimiter)['repeated_list']
		retDict['repeat_tokens'] = tempList
		if len(tempList) > 0:
			print("\nWarning: Following tokens appear in the spec multiple times")
			tempList = (tempList)
			print(json.dumps(tempList, indent=2))
		else:
			print("No tokens were reapeted in spec")
	
	if 'all' in test_list or 'extra_inferred' in test_list:
		tempList = find_extra_inferred_properties(specDict)
		retDict['extra_inferred'] = tempList
		if len(tempList) > 0:
			print("\nError: Following properties appear in inferredSpec section but not in pvs")
			tempList = list(set(tempList))
			print(json.dumps(tempList, indent=2))
		else:
			print("No extra inferredSpec")

	return retDict

def run_tests_column_dict(columnsDict, specDict, test_list=['all'], outputPath='../output/', filewise=False, showSummary=False, delimiter='!!'):
	
	testResults = {}
	for filename in columnsDict:
		if filename != 'all':
			curColumns = columnsDict[filename]['column_list']
			columnsDict[filename]['ignored_column_list'] = ignored_columns(curColumns, specDict, delimiter)
			columnsDict[filename]['accepted_column_list'] = remove_columns_to_be_ignored(curColumns, specDict, delimiter)
			columnsDict[filename]['accepted_token_list'] = get_tokens_list_from_column_list(columnsDict[filename]['accepted_column_list'], delimiter)
			columnsDict[filename]['column_list_count'] = len(curColumns)
			columnsDict[filename]['ignored_column_count'] = len(columnsDict[filename]['ignored_column_list'])
			columnsDict[filename]['accepted_column_count'] = len(columnsDict[filename]['accepted_column_list'])

			#run the tests if flag raised
			if filewise:
				print('----------------------------------------------------')
				print(filename)
				print('----------------------------------------------------')
				testResults[filename] = test_column_name_list(curColumns, specDict, test_list, True, delimiter)
				print('Total Number of Columns', columnsDict[filename]['column_list_count'])
				print('Total Number of Ignored Columns', columnsDict[filename]['ignored_column_count'])
				print('Total Number of Accepted Columns', columnsDict[filename]['accepted_column_count'])
	
	
	allColumns = columnsDict['all']['column_list']
	# if filewise outputs have not been shown or summary is requested
	if not filewise or showSummary:
		testResults['all'] = test_column_name_list(allColumns, specDict, test_list, False, delimiter)
	testResults['all'].update(test_spec(allColumns, specDict, test_list, delimiter))

	
	columnsDict['all']['ignored_column_list'] = ignored_columns(allColumns, specDict, delimiter)
	columnsDict['all']['accepted_column_list'] = remove_columns_to_be_ignored(allColumns, specDict, delimiter)
	columnsDict['all']['accepted_token_list'] = get_tokens_list_from_column_list(columnsDict['all']['accepted_column_list'], delimiter)
	columnsDict['all']['column_list_count'] = len(allColumns)
	columnsDict['all']['ignored_column_count'] = len(columnsDict['all']['ignored_column_list'])
	columnsDict['all']['accepted_column_count'] = len(columnsDict['all']['accepted_column_list'])

	print('Total Number of Columns', columnsDict['all']['column_list_count'])
	print('Total Number of Ignored Columns', columnsDict['all']['ignored_column_count'])
	print('Total Number of Accepted Columns', columnsDict['all']['accepted_column_count'])

	print('creating output files')

	with open(os.path.join(outputPath, 'columns.json'), 'w') as fp:
		json.dump(columnsDict, fp, indent=2)
	with open(os.path.join(outputPath, 'test_results.json'), 'w') as fp:
		json.dump(testResults, fp, indent=2)

	print("End of test")

	
# assumes all files are data overlay type if not flagged	
def test_CSVfile_list(csvPathList, specPath, test_list=['all'], outputPath='../output/', filewise=False, showSummary=False, isMetadata = [False], delimiter='!!'):
	# clean the file paths
	specPath = os.path.expanduser(specPath)
	outputPath = os.path.expanduser(outputPath)
	if not os.path.exists(outputPath):
		os.makedirs(outputPath, exist_ok=True)

	# read json spec
	specDict = get_spec_dict_from_path(specPath)
	allColumns = []

	columnsDict = {}
	testResults = {}

	# assume data overlays file if insufficient information is present
	if len(isMetadata) < len(csvPathList):
		for i in range(0,(len(csvPathList)-len(isMetadata))):
			isMetadata.append(False)
	
	print("Testing ", csvPathList, "against spec at", specPath)
	
	# compile list of columns and run tests on individual files if flag set
	for i, filename in enumerate(csvPathList):
		# clean the file paths
		filename = os.path.expanduser(filename)
		# create csv reader
		csvReader = csv.reader(open(filename, 'r'))
		curColumns = columns_from_CSVreader(csvReader, isMetadata[i])
		allColumns.extend(curColumns)

		columnsDict[filename] = {}
		columnsDict[filename]['column_list'] = curColumns
		
	# keep unique columns
	allColumns = list(set(allColumns))
	columnsDict['all'] = {}
	columnsDict['all']['column_list'] = allColumns
	
	run_tests_column_dict(columnsDict, specDict, test_list, outputPath, filewise, showSummary, delimiter)
	

#TODO this will overwrite outputs if filenames repeat across zip files
def test_zip_file_list(zipPathList, specPath, test_list=['all'], outputPath='../output/', filewise=False, showSummary=False, checkMetadata = False, delimiter='!!'):
	# clean the file paths
	specPath = os.path.expanduser(specPath)
	outputPath = os.path.expanduser(outputPath)
	if not os.path.exists(outputPath):
		os.makedirs(outputPath, exist_ok=True)

	# read json spec
	specDict = get_spec_dict_from_path(specPath)
	allColumns = []

	columnsDict = {}
	testResults = {}

	for zipPath in zipPathList:
		zipPath = os.path.expanduser(zipPath)
		print("Testing ", zipPath, "against spec at", specPath)
		with zipfile.ZipFile(zipPath) as zf:
			# compile list of columns and run tests on individual files if flag set
			for filename in zf.namelist():
				tempFlag = False
				
				if checkMetadata:
					if '_metadata_' in filename:
						tempFlag = True
				elif '_data_' in filename:
					tempFlag = True
				if tempFlag:
					with zf.open(filename, 'r') as data_f:
						csvReader = csv.reader(io.TextIOWrapper(data_f, 'utf-8'))
						curColumns = columns_from_CSVreader(csvReader, False)
						allColumns.extend(curColumns)
						
						columnsDict[filename] = {}
						columnsDict[filename]['column_list'] = curColumns
						
	# keep unique columns
	allColumns = list(set(allColumns))
	columnsDict['all'] = {}
	columnsDict['all']['column_list'] = allColumns
	
	run_tests_column_dict(columnsDict, specDict, test_list, outputPath, filewise, showSummary, delimiter)
	

def test_column_list(columnListPath, specPath, test_list=['all'], outputPath='../output/', delimiter='!!'):
	# clean the file paths
	columnListPath = os.path.expanduser(columnListPath)
	specPath = os.path.expanduser(specPath)
	outputPath = os.path.expanduser(outputPath)
	if not os.path.exists(outputPath):
		os.makedirs(outputPath, exist_ok=True)

	# read json spec
	specDict = get_spec_dict_from_path(specPath)
	allColumns = json.load(open(columnListPath, 'r'))

	columnsDict = {}
	testResults = {}

	print("Testing ", columnListPath, "against spec at", specPath)

	columnsDict['all'] = {}
	columnsDict['all']['column_list'] = allColumns

	run_tests_column_dict(columnsDict, specDict, test_list, outputPath, False, True, delimiter)
	

def main(argv):
	if FLAGS.csv_path_list:
		test_CSVfile_list(FLAGS.csv_path_list, FLAGS.spec_path, FLAGS.tests, FLAGS.validator_output_path, False, False, [FLAGS.is_metadata], FLAGS.delimiter)
	if FLAGS.zip_path_list:
		test_zip_file_list(FLAGS.zip_path_list, FLAGS.spec_path, FLAGS.tests , FLAGS.validator_output_path, False, False, FLAGS.is_metadata, FLAGS.delimiter)
	if FLAGS.column_list_path:
		test_column_list(FLAGS.column_list_path, FLAGS.spec_path, FLAGS.tests, FLAGS.validator_output_path, FLAGS.delimiter)

if __name__ == '__main__':
	flags.mark_flags_as_required(['spec_path'])
	flags.mark_flags_as_mutual_exclusive(['csv_path_list', 'zip_path_list', 'column_list_path'], required=True)
	app.run(main)

