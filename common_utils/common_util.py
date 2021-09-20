import requests
import json
import zipfile
import csv
import io

def request_url_json(url):
    req = requests.get(url)
    print(req.url)
    if req.status_code == requests.codes.ok:
        response_data = req.json()
        #print(response_data)
    else:
        response_data = {}
        print("HTTP status code: "+str(req.status_code))
        #if req.status_code != 204:
            #TODO
    return response_data

def getTokensListFromZip(zipFilePath, checkMetadata = False, printDetails=False):
    # tokens = set()
    tokens = []
    with zipfile.ZipFile(zipFilePath) as zf:
        for filename in zf.namelist():
            tempFlag = False
            if checkMetadata:
                if '_metadata_' in filename:
                    tempFlag = True
            elif '_data_' in filename:
                tempFlag = True
            if tempFlag:
                if printDetails:
                    print('----------------------------------------------------')
                    print(filename)
                    print('----------------------------------------------------')
                with zf.open(filename, 'r') as data_f:
                    csv_reader = csv.reader(io.TextIOWrapper(data_f, 'utf-8'))
                    for row in csv_reader:
                        if csv_reader.line_num > 2:
                            for tok in row[1].split('!!'):
                                # tokens.add(tok)
                                if tok not in tokens:
                                    tokens.append(tok)
                                    if printDetails:
                                        print(tok)
    # return list(tokens)
    return tokens

def tokenInListIgnoreCase(token, list_check):
    for tok in list_check:
        if tok.lower() == token.lower():
            return True
    return False

def tokenNotInListIgnoreCase(token, list_check):
    for tok in list_check:
        if tok.lower() == token.lower():
            return False
    return True

def columnToBeIgnored(columnName, specDict):
    retValue = False
    if 'ignoreColumns' in specDict:
        for ignoreToken in specDict['ignoreColumns']:
            if '!!' in ignoreToken and ignoreToken == columnName:
                retValue = True
            elif tokenInListIgnoreCase(ignoreToken, columnName.split('!!')):
                retValue = True
    return retValue

def removeColumnsToBeIgnored(columnNameList, specDict):
    retList = []
    for columnName in columnNameList:
        if not columnToBeIgnored(columnName, specDict):
            retList.append(columnName)
    return retList

def ignoredColumns(columnNameList, specDict):
    retList = []
    for columnName in columnNameList:
        if columnToBeIgnored(columnName, specDict):
            retList.append(columnName)
    return retList

# assumes columnNameList does not contain columns to be ignored
def getTokensListFromColumnList(columnNameList):
    # tokens = set()
    tokens = []
    for columnName in columnNameList:
        for tok in columnName.split('!!'):
            # tokens.add(tok)
            if tok not in tokens:
                tokens.append(tok)

    # return list(tokens)
    return tokens

def findMissingTokens(tokenList, specDict):
    tokensCopy = tokenList.copy()
    for token in tokenList:
        for prop in specDict['pvs'].keys():
            if tokenInListIgnoreCase(token, specDict['pvs'][prop]):
                if token in tokensCopy:
                    tokensCopy.remove(token)
                else:
                    print("Warning:", token, "appears multiple times")
        if 'ignoreTokens' in specDict and tokenInListIgnoreCase(token, specDict['ignoreTokens']):
            tokensCopy.remove(token)
        if 'measurement' in specDict and tokenInListIgnoreCase(token, specDict['measurement']):
            tokensCopy.remove(token)
        if 'populationType' in specDict and tokenInListIgnoreCase(token, specDict['populationType']):
            tokensCopy.remove(token)

    return tokensCopy

# assumes metadata file or data with overlays file
def columnsFromCSVReader(csvReader, isMetadataFile = False):
    columnNameList = []
    for row in csvReader:
        if isMetadataFile:
            if len(row) > 1:
                columnNameList.append(row[1])
        else:
            if csvReader.line_num == 2:
                columnNameList = row.copy()
    return columnNameList

# assumes metadata file or data with overlays file
def columnsFromCSVFile(csvPath, isMetadataFile = False):
    csvReader = csv.reader(open(csvPath, 'r'))
    allColumns = columnsFromCSVReader(csvReader, isMetadataFile)

    return allColumns

# assumes metadata file or data with overlays file
def columnsFromCSVFileList(csvPathList, isMetadata = [False]):
    allColumns = []

    if len(isMetadata) < len(csvPathList):
        for i in range(0,(len(csvPathList)-len(isMetadata))):
            isMetadata.append(False)

    for i, curFile in enumerate(csvPathList):
        # create csv reader
        csvReader = csv.reader(open(csvPath, 'r'))
        curColumns = columnsFromCSVReader(csvReader, isMetadata[i])
        allColumns.extend(curColumns)

    allColumns = list(set(allColumns))

    return allColumns

# assumes metadata file or data with overlays file
def columnsFromZipFile(zipPath, checkMetadata = False):
    allColumns = []

    with zipfile.ZipFile(zipPath) as zf:
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

    allColumns = list(set(allColumns))

    return allColumns


def getSpecDictFromPath(specPath):
    with open(specPath, 'r') as fp:
        specDict = json.load(fp)
    return specDict