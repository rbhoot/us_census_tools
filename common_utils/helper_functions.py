from common_util import *
from functools import cmp_to_key

def findColumnsWithToken(columnList, token, delimiter='!!'):
	retList = []
	for curColumn in columnList:
		if token_in_list_ignore_case(token, curColumn.split(delimiter)):
			retList.append(curColumn)
	return list(set((retList)))

def replaceTokenInColumn(curColumn, oldToken, newToken, delimiter='!!'):
	return delimiter.join([newToken if x==oldToken else x for x in curColumn.split(delimiter)])

def replaceFirstTokenInColumn(curColumn, oldToken, newToken, delimiter='!!'):
	new_list = []
	tempFlag = True
	for x in curColumn.split(delimiter):
		if x == oldToken and tempFlag:
			new_list.append(newToken)
			tempFlag = False
		else:
			new_list.append(x)
	
	return delimiter.join(new_list)

# replace token
def replaceTokenInColumnList(columnList, oldToken, newToken, delimiter='!!'):
	retList = []
	for curColumn in columnList:
		retList.append(replaceTokenInColumn(curColumn, oldToken, newToken, delimiter))
	return retList

# combined replace list
def replaceTokenListInColumnList(columnList, oldToken, newTokenList, delimiter='!!'):
	retDict = {}
	for curColumn in columnList:
		retDict[curColumn] = []
		for newToken in newTokenList:
			retDict[curColumn].append(replaceTokenInColumn(curColumn, oldToken, newToken, delimiter))
	return retDict

# find columns sub token
def findColumnsWithTokenPartialMatch(columnList, tokenStr, delimiter='!!'):
	retList = []
	for curColumn in columnList:
		for token in curColumn.split(delimiter):
			if tokenStr.lower() in token.lower():
				retList.append(curColumn)
	return list(set((retList)))

def getcolumnsByTokenCount(columnList, delimiter='!!'):
	retDict = {}
	for curColumn in columnList:
		tokenList = curColumn.split(delimiter)
		if len(tokenList) not in retDict:
			retDict[len(tokenList)] = []
		retDict[len(tokenList)].append(curColumn)
	return retDict

def getColumnsWithSamePrefix(columnsByLength, maxExtraToken=1, delimiter='!!'):
	retDict = {}
	for columnLength in columnsByLength:
		for curColumn in columnsByLength[columnLength]:
			extraLength = 1
			while extraLength <= maxExtraToken:
				if (columnLength+extraLength) in columnsByLength: 
					for comapreColumn in columnsByLength[columnLength+extraLength]:
						if curColumn in comapreColumn:
							if curColumn not in retDict:
								retDict[curColumn] = []
							retDict[curColumn].append(comapreColumn)
				extraLength += 1
	return retDict

def guessTotalColumnsFromZipFile(zipPath):
	zipPath = os.path.expanduser(zipPath)
	totalColumns = []

	with zipfile.ZipFile(zipPath) as zf:
		for filename in zf.namelist():
			if '_data_' in filename:
				with zf.open(filename, 'r') as data_f:
					csvReader = csv.reader(io.TextIOWrapper(data_f, 'utf-8'))
					for row in csvReader:
						if csvReader.line_num == 2:
							columnNameList = row.copy()
						elif csvReader.line_num == 3:
							for i, val in enumerate(row):
								try:
									if float(val) > 100:
										if 'Margin of Error' not in columnNameList[i] and 'Median' not in columnNameList[i] and 'Mean' not in columnNameList[i] and 'INCOME' not in columnNameList[i]:
											if columnNameList[i] not in totalColumns:
												totalColumns.append(columnNameList[i])
								except ValueError:
									pass
	return totalColumns

def columnFindPrefixed(column_name, prefix_list):
	matched_prefix = None
	if column_name not in prefix_list:
		for cur_prefix in prefix_list:
			if len(cur_prefix) < len(column_name) and cur_prefix in column_name:
				if matched_prefix:
					if len(cur_prefix) > len(matched_prefix):
						matched_prefix = cur_prefix
				else:
					matched_prefix = cur_prefix
	
	return matched_prefix

delimiter = '!!'
# all_columns = columnsFromZipFile('~/acs_tables/S2603/S2603_us.zip')
# spec_dict = getSpecDictFromPath('../spec_dir/S2603_spec.json')
all_columns = columns_from_zip_file('~/acs_tables/S2602/S2602_us.zip')
spec_dict = get_spec_dict_from_path('../spec_dir/S2602_spec.json')

all_columns = remove_columns_to_be_ignored(all_columns, spec_dict)

# print(json.dumps(getcolumnsByTokenCount(all_columns), indent=2))
# print(json.dumps(getColumnsWithSamePrefix(getcolumnsByTokenCount(all_columns), 2), indent=2))

# total_columns = guessTotalColumnsFromZipFile('~/acs_tables/S2603/S2603_us.zip')
total_columns = guessTotalColumnsFromZipFile('~/acs_tables/S2602/S2602_us.zip')

# total_columns = removeColumnsToBeIgnored(total_columns, spec_dict)

# print(json.dumps(total_columns, indent=2))

# column_names = ['Total population', 'Total group quarters population', 'Adult correctional facilities', 'Nursing facilities/skilled nursing facilities', 'Juvenile Facilities', 'College/university housing', 'Military quarters/military ships']
# replace_columns_names = ['Total population', 'Adult correctional facilities', 'Nursing facilities/skilled nursing facilities', 'Juvenile Facilities', 'College/university housing', 'Military quarters/military ships']
column_names = ['Total population', 'Total group quarters population', 'Adult correctional facilities', 'Nursing facilities/skilled nursing facilities', 'College/university housing']
replace_columns_names = ['Total population', 'Adult correctional facilities', 'Nursing facilities/skilled nursing facilities', 'College/university housing']

total_columns = findColumnsWithToken(total_columns, 'Total group quarters population')


# get column names
# get column names to consider for percentage
	# if none, all would be used
# find token index of column name
	# token name or token name + MOE
	# warn columns without token
	# intersection of all
# guess method
	# column to column map
		# all the percent values match same token or token + MOE
	# prefix
		# all the percent values have a column that is non percent and can be prefixed
	# none
		# non prefix matching columns



# replace with other groups to get total_columns 
for column_name in total_columns.copy():
	# temp_str = replaceTokenInColumn(column_name, 'Estimate', 'Margin of Error', delimiter)
	# if temp_str not in all_columns:
	# 	print(temp_str)
	# total_columns.append(temp_str)
	for new_token in replace_columns_names:
		other_column = replaceTokenInColumn(column_name, 'Total group quarters population', new_token, delimiter)
		total_columns.append(other_column)
		# temp_str = replaceTokenInColumn(other_column, 'Estimate', 'Margin of Error', delimiter)
		# total_columns.append(temp_str)
		# if temp_str not in all_columns:
		# 	print(temp_str)

denominators = {}
for cur_column in all_columns:
	# TODO create other list of prefixes
	if 'Median' not in cur_column and 'Mean' not in cur_column:
		prefix = columnFindPrefixed(cur_column, total_columns)
		if prefix:
			if prefix not in denominators:
				denominators[prefix] = []
			denominators[prefix].append(cur_column)
			temp_str = replaceTokenInColumn(cur_column, 'Estimate', 'Margin of Error', delimiter)
			if temp_str not in all_columns:
				print('Column expected but not found:', temp_str)
			for column_group in column_names:
				temp_str = replaceTokenInColumn(cur_column, 'Estimate', 'Margin of Error', delimiter)
				temp_str = replaceFirstTokenInColumn(temp_str, column_group, column_group+' MOE', delimiter)
				if temp_str in all_columns and temp_str not in denominators[prefix]:
					denominators[prefix].append(temp_str)
				# if temp_str not in all_columns:
					# print('Column expected but not found:', temp_str)
					# print(temp_str)
			if temp_str in all_columns and temp_str not in denominators[prefix]:
				denominators[prefix].append(temp_str)
			# if temp_str not in all_columns:
				# print('Column expected but not found:', temp_str)
				# print(temp_str)

# print(json.dumps(sorted(total_columns), indent=2))
# print(json.dumps(list(denominators.keys()), indent=2))
print(json.dumps(dict(sorted(denominators.items())), indent=2))

total_moe_columns = findColumnsWithToken(all_columns, 'Margin of Error')
median_moe_columns = findColumnsWithTokenPartialMatch(total_moe_columns, 'Median')
mean_moe_columns = findColumnsWithTokenPartialMatch(total_moe_columns, 'Mean')

print(json.dumps(list(set(median_moe_columns).union(set(mean_moe_columns))), indent=2))

# add columns that have substring but not equal len(a) < len(b) and a in b
# replace estimate with margin of error, substring
# replace column name with column name + MOE, substring

# total_moe_columns = findColumnsWithToken(total_columns, 'Margin of Error')
# median_columns = findColumnsWithTokenPartialMatch(total_columns, 'Median')
# mean_columns = findColumnsWithTokenPartialMatch(total_columns, 'Mean')

# only_totals = list(set(total_columns)-set(total_moe_columns)-set(median_columns)-set(mean_columns))



# add columns that have substring but not equal len(a) < len(b) and a in b
# replace estimate with margin of error, 
# replace column name with column name + MOE

# print(json.dumps(sorted(only_totals, key=cmp_to_key(compare)), indent=2))

# all_columns = columnsFromZipFile('~/acs_tables/S0701PR/S0701PR_us.zip')
# spec_dict = getSpecDictFromPath('../spec_dir/S0701PR_spec.json')
# # ign_list = []
# # for curColumn in all_columns:
# # 	if curColumn.endswith('!!MARITAL STATUS!!Population 15 years and over'):
# # 		ign_list.append(curColumn)
# # print(json.dumps(ign_list, indent=2))

# # for curColumn in all_columns:
# # 	if 'Population 15 years and over!!$75,000 or more' in curColumn and 'Margin of Error' in curColumn and 'Total' in curColumn:
# # 		print(curColumn)
# # moe_columns = findColumnsWithToken(all_columns, 'Margin of Error')

# # get all columns

# all_columns = removeColumnsToBeIgnored(all_columns, spec_dict)
# total_columns = findColumnsWithToken(all_columns, 'Total')
# moe_columns = findColumnsWithToken(all_columns, 'Margin of Error')

# #remove median related columns
# median_columns = findColumnsWithTokenPartialMatch(all_columns, 'Median')
# median_moe_columns =findColumnsWithTokenPartialMatch(median_columns, 'Margin of Error')
# # print(json.dumps(median_moe_columns, indent = 2))
# print(len(median_columns))
# total_columns = list(set(total_columns)-set(median_columns))
# total_columns = list(set(total_columns)-set(moe_columns))

# denominators_section = {}
# # total_replacements = ['Moved; within same county', 'Moved; from different county, same state', 'Moved; from different  state', 'Moved; from abroad']
# total_replacements = ['Moved; within same municipio', 'Moved; from different municipio', 'Moved; from the U.S.', 'Moved; from outside Puerto Rico and the U.S.']

# # denominators_section = replaceTokenListInColumnList(total_columns, 'Total', ['Moved; within same county', 'Moved; from different county, same state', 'Moved; from different  state', 'Moved; from abroad'])
# for column_name in total_columns:
# 	denominators_section[column_name] = []
# 	for token in total_replacements:
# 		new_column = replaceTokenInColumn(column_name, 'Total', token)
# 		new_column_moe = replaceTokenInColumn(column_name, 'Total', token+' MOE')
# 		if new_column in all_columns:
# 			denominators_section[column_name].append(new_column)
# 		temp_str = replaceTokenInColumn(new_column, 'Estimate', 'Margin of Error')
# 		if temp_str in all_columns:
# 			denominators_section[column_name].append(temp_str)
# 		temp_str = replaceTokenInColumn(new_column_moe, 'Estimate', 'Margin of Error')
# 		if temp_str in all_columns:
# 			denominators_section[column_name].append(temp_str)
# # denominators_section = replaceTokenListInColumnList(total_columns, 'Total', ['Moved; within same municipio', 'Moved; from different municipio', 'Moved; from the U.S.', 'Moved; from outside Puerto Rico and the U.S.'])

# print(json.dumps(denominators_section, indent = 2))

# total_moe_columns = findColumnsWithToken(all_columns, 'Total MOE')
# median_moe_columns = findColumnsWithTokenPartialMatch(total_moe_columns, 'Median')
# print(len(median_moe_columns))
# print(json.dumps(median_moe_columns, indent = 2))
# total_moe_columns = list(set(total_moe_columns)-set(median_moe_columns))
# # denominators_section.update(replaceTokenListInColumnList(total_moe_columns, 'Total MOE', ['Moved; within same county MOE', 'Moved; from different county, same state MOE', 'Moved; from different  state MOE', 'Moved; from abroad MOE']))
# denominators_section.update(replaceTokenListInColumnList(total_moe_columns, 'Total MOE', ['Moved; within same municipio MOE', 'Moved; from different municipio MOE', 'Moved; from the U.S. MOE', 'Moved; from outside Puerto Rico and the U.S. MOE']))


# print(json.dumps(denominators_section, indent = 2))

# median_moe = findColumnsWithToken(median_columns, 'Margin of Error')
# median_est = findColumnsWithToken(median_columns, 'Estimate')
# replaced_columns = replaceTokenInColumnList(median_est, 'Estimate', 'Margin of Error')
# print('margin', len(findColumnsWithToken(median_columns, 'Margin of Error')))
# print('estimate', len(findColumnsWithToken(median_columns, 'Estimate')))
# print(len(findColumnsWithTokenPartialMatch(total_moe_columns, 'Median')))
# print(list(set(median_moe)-set(replaced_columns)))

# print(list(set(total_columns)-set(median_columns)))
# total_columns = list(set(total_columns)-set(median_columns))
# replaced_columns = replaceTokenInColumnList(total_columns, 'Estimate', 'Margin of Error')
# moe_columns = findColumnsWithToken(all_columns, 'Margin of Error')

# print(json.dumps(total_columns, indent=1))
# print(json.dumps(list(set(replaced_columns)-set(moe_columns)), indent=2))

# print(len(total_moe_columns))
# print(len(findColumnsWithToken(median_columns, 'Margin of Error')))
# print(len(findColumnsWithTokenPartialMatch(all_columns, 'Median')))


