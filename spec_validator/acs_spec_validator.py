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

flags.DEFINE_string('validator_output_path', './outputs/', 'Path to store the output files')
flags.DEFINE_multi_enum('tests', ['all'], ['all', 'extra_tokens', 'missing_tokens', 'column_no_pv', 'ignore_conflicts', 'enum_specialialisations', 'denominators', 'extra_inferred'], 'List of tests to run')

# finds any extra tokens that appear in the spec as a lookup but not as a part of any of the column names
# requires column list before ignored columns are removed

def findExtraTokens(columnNameList, specDict, delimiter='!!'):
	retList = []
	# get list of unique tokens across all columns 
	tokenList = getTokensListFromColumnList(columnNameList, delimiter)
	
	retList = getSpecTokenList(specDict, delimiter)['token_list']
	
	tokensCopy = retList.copy()
	
	# ignore tokens beginning with an underscore or if token is a column name and appears in columnNameList
	for token in tokensCopy:
		if token.startswith('_'):
			retList.remove(token)
		elif tokenInListIgnoreCase(token, tokenList):
			retList.remove(token)
		if delimiter in token:
			if token in columnNameList:
				retList.remove(token)
	return retList


# finds all columns that do not assign any property a value
# assumes columnNameList does not contain columns to be ignored
def findColumnsWithNoProperties(columnNameList, specDict, delimiter='!!'):
	retList = []
	for columnName in columnNameList:
		noPropFlag = True
		# get token list of the column
		for token in columnName.split(delimiter):
			for prop in specDict['pvs'].keys():
				if tokenInListIgnoreCase(token, specDict['pvs'][prop].keys()):
					# clear the flag when some property gets assigned a value
					noPropFlag = False
		# if the flag has remained set across all properties
		if noPropFlag:
			retList.append(columnName)
	return retList

# returns list of tokens that appear in ignoreColumn as well as a PV
# checks only tokens, ignores long column names
def findIgnoreConflicts(specDict, delimiter='!!'):
	retList = []
	
	newDict = copy.deepcopy(specDict)
	newDict.pop('ignoreColumns', None)
	newDict.pop('ignoreTokens', None)

	specTokens = getSpecTokenList(newDict, delimiter)['token_list']

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
def findMissingEnumSpecialisation(columnNameList, specDict, delimiter='!!'):
	retDict = {}
	for columnName in columnNameList:
		tempDict = {}
		# populate a dictionary containing properties and all the values assigned to it
		for token in columnName.split(delimiter):
			for prop in specDict['pvs'].keys():
				if tokenInListIgnoreCase(token, specDict['pvs'][prop].keys()):
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
					# if token appears as a specialisation but it's base doesn't appear before it
					if tokenInListIgnoreCase(propToken, specDict['enumSpecializations']):
						if specDict['enumSpecializations'][propToken] not in tempDict[prop][:j]:
							if propToken not in retDict:
								retDict[propToken] = {}
								retDict[propToken]['column'] = [columnName]
								retDict[propToken]['possibleParents'] = tempDict[prop][:j]
							else:
								retDict[propToken]['column'].append(columnName)
								retDict[propToken]['possibleParents'].extend(tempDict[prop][:j])
					# if the token is near the leaf but not used as a specialisation, it potentially has a base value
					elif j > 0:
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

# check if all the columns that appear as total exist
# assumes columnNameList does not contain columns to be ignored
def findMissingDenominatorTotalColumn(columnNameList, specDict, delimiter = '!!'):
	retList = []
	
	tokenList = getTokensListFromColumnList(columnNameList, delimiter)

	if 'denominators' in specDict:
		for totalColumn in specDict['denominators'].keys():
			if delimiter in totalColumn:
				if totalColumn not in columnNameList:
					retList.append(totalColumn)
			elif not tokenInListIgnoreCase(totalColumn, tokenList):
				retList.append(totalColumn)
	return retList

def findMissingDenominators(columnNameList, specDict, delimiter = '!!'):
	retList = []
	
	tokenList = getTokensListFromColumnList(columnNameList, delimiter)

	if 'denominators' in specDict:
		for totalColumn in specDict['denominators'].keys():
			for curDenominator in specDict['denominators'][totalColumn]:
				if delimiter in curDenominator:
					if curDenominator not in columnNameList:
						retList.append(curDenominator)
				elif not tokenInListIgnoreCase(curDenominator, tokenList):
					retList.append(curDenominator)
	return retList

def findRepeatingDenominators(columnNameList, specDict, delimiter = '!!'):
	retList = []
	appearedList = []

	tokenList = getTokensListFromColumnList(columnNameList, delimiter)

	if 'denominators' in specDict:
		for totalColumn in specDict['denominators'].keys():
			for curDenominator in specDict['denominators'][totalColumn]:
				if tokenInListIgnoreCase(curDenominator, appearedList):
					retList.append(curDenominator)
				else:
					appearedList.append(curDenominator)
	return retList
	

# runs all the tests related to tokens and columns and prints relevant output
# requires column list before ignored columns are removed
# raiseWarningsOnly is used mainly in case of yearwise processing, it prevents raising of false errors
def testColumnNameList(columnNameList, specDict, test_list=['all'], raiseWarningsOnly = False, delimiter = '!!'):
	retDict = {}

	# remove ignore columns
	columnNameList = removeColumnsToBeIgnored(columnNameList, specDict, delimiter)
	
	tokenList = getTokensListFromColumnList(columnNameList, delimiter)
	
	if 'all' in test_list or 'missing_tokens' in test_list:
		tempList = findMissingTokens(tokenList, specDict)
		retDict['missing_tokens'] = []
		if len(tempList) > 0:
			print("\nWarning: Following tokens are missing in the spec")
			tempList = list(set(tempList))
			print(json.dumps(tempList, indent=2))
			retDict['missing_tokens'] = tempList
		else:
			print("All tokens present in spec or ignored")
	
	if 'all' in test_list or 'column_no_pv' in test_list:
		tempList = findColumnsWithNoProperties(columnNameList, specDict)
		retDict['no_pv_columns'] = []
		if len(tempList) > 0:
			print("\nWarning: Following columns do not have any property assigned")
			tempList = list(set(tempList))
			print(json.dumps(tempList, indent=2))
			retDict['no_pv_columns'] = tempList
		else:
			print("All columns have PV assignment")
	
	if 'all' in test_list or 'ignore_conflicts' in test_list:
		tempList = findIgnoreConflicts(specDict)
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
		tempDict = findMissingEnumSpecialisation(columnNameList, specDict)
		if len(tempDict) > 0:
			print("\nWarning: Following tokens should have an enumSpecialization")
			print(json.dumps(tempDict, indent=2))
			retDict['enum_specializations_missing'] = tempDict
		else:
			print("All tokens in enumSpecialization found")

	if 'all' in test_list or 'denominators' in test_list:
		tempList = findMissingDenominatorTotalColumn(columnNameList, specDict, delimiter)
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

		tempList = findMissingDenominators(columnNameList, specDict, delimiter)
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

		tempList = findRepeatingDenominators(columnNameList, specDict, delimiter)
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

	return retDict

def findExtraInferredProperties(specDict):
	retList = []
	if 'inferredSpec' in specDict:
		for property_name in specDict['inferredSpec']:
			if property_name not in specDict['pvs']:
				retList.append(property_name)
	return retList

# calls all methods to check the spec 
def testSpec(columnNameList, specDict, test_list=['all'], delimiter='!!'):
	retDict = {}
	if 'all' in test_list or 'extra_tokens' in test_list:
		tempList = findExtraTokens(columnNameList, specDict)
		retDict['extra_tokens'] = tempList
		if len(tempList) > 0:
			print("\nError: Following tokens appear in the spec but not in csv")
			tempList = list(set(tempList))
			print(json.dumps(tempList, indent=2))
		else:
			print("No extra tokens in spec")

		tempList = getSpecTokenList(specDict, delimiter)['repeated_list']
		retDict['repeat_tokens'] = tempList
		if len(tempList) > 0:
			print("\nWarning: Following tokens appear in the spec multiple times")
			tempList = (tempList)
			print(json.dumps(tempList, indent=2))
		else:
			print("No tokens were reapeted in spec")
	
	if 'all' in test_list or 'extra_inferred' in test_list:
		tempList = findExtraInferredProperties(specDict)
		retDict['extra_inferred'] = tempList
		if len(tempList) > 0:
			print("\nError: Following properties appear in inferredSpec section but not in pvs")
			tempList = list(set(tempList))
			print(json.dumps(tempList, indent=2))
		else:
			print("No extra inferredSpec")

	return retDict

# run all the tests on a single csv file
# tests data overlay by default, isMetadata = True parses assuming csv file is metadata file
def testCSVFile(csvPath, specPath, test_list=['all'], outputPath='./outputs/', isMetadata = False, delimiter='!!'):
	# clean the file paths
	csvPath = os.path.expanduser(csvPath)
	specPath = os.path.expanduser(specPath)
	outputPath = os.path.expanduser(outputPath)
	if not os.path.exists(outputPath):
		os.makedirs(outputPath, exist_ok=True)

	print("Testing ", csvPath, "against spec at", specPath)
	
	columnsDict = {}

	# read json spec
	specDict = getSpecDictFromPath(specPath)
	# create csv reader
	csvReader = csv.reader(open(csvPath, 'r'))
	# compile list of columns
	allColumns = columnsFromCSVReader(csvReader, isMetadata)

	# run the tests
	testResults = {}
	testResults['all'] = testColumnNameList(allColumns, specDict, test_list, False, delimiter)

	testResults['all'].update(testSpec(allColumns, specDict, test_list, delimiter))

	columnsDict['all'] = {}
	columnsDict['all']['column_list'] = allColumns
	columnsDict['all']['ignored_column_list'] = ignoredColumns(allColumns, specDict, delimiter)
	columnsDict['all']['accepted_column_list'] = removeColumnsToBeIgnored(allColumns, specDict, delimiter)
	columnsDict['all']['accepted_token_list'] = getTokensListFromColumnList(columnsDict['all']['accepted_column_list'], delimiter)
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

	
# assumes all files are metadata type if not flagged	
def testCSVFileList(csvPathList, specPath, test_list=['all'], outputPath='./outputs/', filewise=False, showSummary=False, isMetadata = [False], delimiter='!!'):
	# clean the file paths
	specPath = os.path.expanduser(specPath)
	outputPath = os.path.expanduser(outputPath)
	if not os.path.exists(outputPath):
		os.makedirs(outputPath, exist_ok=True)

	# read json spec
	specDict = getSpecDictFromPath(specPath)
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
		curColumns = columnsFromCSVReader(csvReader, isMetadata[i])
		allColumns.extend(curColumns)

		columnsDict[filename] = {}
		columnsDict[filename]['column_list'] = curColumns
		columnsDict[filename]['ignored_column_list'] = ignoredColumns(curColumns, specDict, delimiter)
		columnsDict[filename]['accepted_column_list'] = removeColumnsToBeIgnored(curColumns, specDict, delimiter)
		columnsDict[filename]['accepted_token_list'] = getTokensListFromColumnList(columnsDict[filename]['accepted_column_list'], delimiter)
		columnsDict[filename]['column_list_count'] = len(curColumns)
		columnsDict[filename]['ignored_column_count'] = len(columnsDict[filename]['ignored_column_list'])
		columnsDict[filename]['accepted_column_count'] = len(columnsDict[filename]['accepted_column_list'])

		#run the tests if flag raised
		if filewise:
			print('----------------------------------------------------')
			print(filename)
			print('----------------------------------------------------')
			testResults[filename] = testColumnNameList(curColumns, specDict, test_list, True, delimiter)
			print('Total Number of Columns', columnsDict[filename]['column_list_count'])
			print('Total Number of Ignored Columns', columnsDict[filename]['ignored_column_count'])
			print('Total Number of Accepted Columns', columnsDict[filename]['accepted_column_count'])
	# keep unique columns
	allColumns = list(set(allColumns))

	# if filewise outputs have not been shown or summary is requested
	if not filewise or showSummary:
		testResults['all'] = testColumnNameList(allColumns, specDict, test_list, False, delimiter)
	testResults['all'].update(testSpec(allColumns, specDict, test_list, delimiter))

	columnsDict['all'] = {}
	columnsDict['all']['column_list'] = allColumns
	columnsDict['all']['ignored_column_list'] = ignoredColumns(allColumns, specDict, delimiter)
	columnsDict['all']['accepted_column_list'] = removeColumnsToBeIgnored(allColumns, specDict, delimiter)
	columnsDict['all']['accepted_token_list'] = getTokensListFromColumnList(columnsDict['all']['accepted_column_list'], delimiter)
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
	

def testZipFile(zipPath, specPath, test_list=['all'], outputPath='./outputs/', filewise=False, showSummary=False, checkMetadata = False, delimiter='!!'):
	# clean the file paths
	zipPath = os.path.expanduser(zipPath)
	specPath = os.path.expanduser(specPath)
	outputPath = os.path.expanduser(outputPath)
	if not os.path.exists(outputPath):
		os.makedirs(outputPath, exist_ok=True)

	# read json spec
	specDict = getSpecDictFromPath(specPath)
	allColumns = []

	columnsDict = {}
	testResults = {}

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
					curColumns = columnsFromCSVReader(csvReader, False)
					allColumns.extend(curColumns)
					
					columnsDict[filename] = {}
					columnsDict[filename]['column_list'] = curColumns
					columnsDict[filename]['ignored_column_list'] = ignoredColumns(curColumns, specDict, delimiter)
					columnsDict[filename]['accepted_column_list'] = removeColumnsToBeIgnored(curColumns, specDict, delimiter)
					columnsDict[filename]['accepted_token_list'] = getTokensListFromColumnList(columnsDict[filename]['accepted_column_list'], delimiter)
					columnsDict[filename]['column_list_count'] = len(curColumns)
					columnsDict[filename]['ignored_column_count'] = len(columnsDict[filename]['ignored_column_list'])
					columnsDict[filename]['accepted_column_count'] = len(columnsDict[filename]['accepted_column_list'])

					#run the tests if flag raised
					if filewise:
						print('----------------------------------------------------')
						print(filename)
						print('----------------------------------------------------')
						testResults[filename] = testColumnNameList(curColumns, specDict, test_list, True, delimiter)
						print('Total Number of Columns', columnsDict[filename]['column_list_count'])
						print('Total Number of Ignored Columns', columnsDict[filename]['ignored_column_count'])
						print('Total Number of Accepted Columns', columnsDict[filename]['accepted_column_count'])
	# keep unique columns
	allColumns = list(set(allColumns))
	# if filewise outputs have not been shown or summary is requested
	if not filewise or showSummary:
		testResults['all'] = testColumnNameList(allColumns, specDict, test_list, False, delimiter)
	testResults['all'].update(testSpec(allColumns, specDict, test_list, delimiter))

	columnsDict['all'] = {}
	columnsDict['all']['column_list'] = allColumns
	columnsDict['all']['ignored_column_list'] = ignoredColumns(allColumns, specDict, delimiter)
	columnsDict['all']['accepted_column_list'] = removeColumnsToBeIgnored(allColumns, specDict, delimiter)
	columnsDict['all']['accepted_token_list'] = getTokensListFromColumnList(columnsDict['all']['accepted_column_list'], delimiter)
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


def main(argv):
    if FLAGS.zip_path:
    	testZipFile(FLAGS.zip_path, FLAGS.spec_path, FLAGS.tests , FLAGS.validator_output_path, False, False, FLAGS.is_metadata, FLAGS.delimiter)
    if FLAGS.csv_path_list:
    	testCSVFileList(FLAGS.csv_path_list, FLAGS.spec_path, FLAGS.tests, FLAGS.validator_output_path, False, False, [FLAGS.is_metadata], FLAGS.delimiter)
    if FLAGS.csv_path:
    	testCSVFile(FLAGS.csv_path, FLAGS.spec_path, FLAGS.tests, FLAGS.validator_output_path, FLAGS.is_metadata, FLAGS.delimiter)


if __name__ == '__main__':
	flags.mark_flags_as_required(['spec_path'])
	flags.mark_flags_as_mutual_exclusive(['zip_path', 'csv_path', 'csv_path_list'], required=True)
	app.run(main)

