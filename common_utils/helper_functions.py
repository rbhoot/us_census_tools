from common_util import *

def findColumnsWithToken(columnList, token, delimiter='!!'):
	retList = []
	for curColumn in columnList:
		if tokenInListIgnoreCase(token, curColumn.split(delimiter)):
			retList.append(curColumn)
	return list(set((retList)))

def replaceTokenInColumn(curColumn, oldToken, newToken, delimiter='!!'):
	return delimiter.join([newToken if x==oldToken else x for x in curColumn.split(delimiter)])

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

all_columns = columnsFromZipFile('~/Downloads/S0701.zip')
# for curColumn in all_columns:
# 	if 'Population 15 years and over!!$75,000 or more' in curColumn and 'Margin of Error' in curColumn and 'Total' in curColumn:
# 		print(curColumn)
# moe_columns = findColumnsWithToken(all_columns, 'Margin of Error')

# get all columns
all_columns = removeColumnsToBeIgnored(all_columns, getSpecDictFromPath('../spec_generator/S0701_spec.json'))
total_columns = findColumnsWithToken(all_columns, 'Total')

#remove median related columns
median_columns = findColumnsWithTokenPartialMatch(all_columns, 'Median')
# print(len(median_columns))
total_columns = list(set(total_columns)-set(median_columns))

denominators_section = replaceTokenListInColumnList(total_columns, 'Total', ['Moved; within same county', 'Moved; from different county, same state', 'Moved; from different  state', 'Moved; from abroad'])

total_moe_columns = findColumnsWithToken(all_columns, 'Total MOE')
median_moe_columns = findColumnsWithTokenPartialMatch(total_moe_columns, 'Median')
total_moe_columns = list(set(total_moe_columns)-set(median_columns))
denominators_section.update(replaceTokenListInColumnList(total_moe_columns, 'Total MOE', ['Moved; within same county MOE', 'Moved; from different county, same state MOE', 'Moved; from different  state MOE', 'Moved; from abroad MOE']))

print(json.dumps(denominators_section, indent = 2))
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


