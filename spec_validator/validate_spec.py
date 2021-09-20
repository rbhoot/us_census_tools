from acs_spec_validator import *

testZipFile(zipPath = '../sample_data/s1702.zip', specPath = '../spec_dir/s1702_spec.json', outputPath = './outputs/', filewise = False)
testCSVFileList(csvPathList = ['../sample_data/s1702/2019_sample.csv', '../sample_data/s1702/2018_sample.csv'], specPath = '../spec_dir/s1702_spec.json', outputPath = './outputs2/', filewise = True, showSummary = True)
testCSVFile(csvPath = '../sample_data/s1702/2017_metadata.csv', specPath = '../spec_dir/s1702_spec.json', outputPath = './outputs3/', isMetadataFile = True)

# specDict = getSpecDictFromPath(specPath)

'''
create csv reader
compile a list of columns from csv reader
remove ignore columns if necessary
run individual test as required
'''
# csvReader = csv.reader(open(csvPath, 'r'))
# allColumns = columnsFromCSVReader(csvReader, isMetadataFile = False)

# tempList = findExtraTokens(columnNameList, specDict)

# columnNameList = removeColumnsToBeIgnored(columnNameList, specDict)
# tokenList = getTokensListFromColumnList(columnNameList)

# tempList = findMissingTokens(tokenList, specDict)

# tempList = findColumnsWithNoProperties(columnNameList, specDict)

# tempList = findIgnoreConflicts(specDict)

# tempDict = findMissingEnumSpecialisation(columnNameList, specDict)

# tempList = findMissingDenominatorTotalColumn(columnNameList, specDict)
