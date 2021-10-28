import requests
import json
import zipfile
import csv
import io
import os
from absl import app
from absl import flags

FLAGS = flags.FLAGS

flags.DEFINE_string('zip_path', None, 'Path to zip file downloaded from US Census')
flags.DEFINE_string('csv_path', None, 'Path to csv file downloaded from US Census')
flags.DEFINE_list('csv_path_list', None, 'List of paths to csv files downloaded from US Census')
flags.DEFINE_string('spec_path', None, 'Path to config spec JSON file')
flags.DEFINE_boolean('get_tokens', False, 'Produce a list of tokens from the input file/s')
flags.DEFINE_boolean('get_columns', False, 'Produce a list of columns from the input file/s')
flags.DEFINE_boolean('get_ignored_columns', False, 'Produce a list of columns ignored from the input file/s according to spec')
flags.DEFINE_boolean('ignore_columns', False, 'Account for columns to be ignored according to the spec')
flags.DEFINE_boolean('is_metadata', False, 'Parses the file assuming it is _metadata_ type file')
flags.DEFINE_string('delimiter', '!!', 'The delimiter to extract tokens from column name')

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

def get_tokens_list_from_zip(zipFilePath, checkMetadata = False, printDetails=False, delimiter='!!'):
    # tokens = set()
    zipFilePath = os.path.expanduser(zipFilePath)
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
                        if checkMetadata:
                            for tok in row[1].split(delimiter):
                                # tokens.add(tok)
                                if tok not in tokens:
                                    tokens.append(tok)
                                    if printDetails:
                                        print(tok)
                        else:
                            if csv_reader.line_num == 2:
                                for column_name in row:
                                    for tok in column_name.split(delimiter):
                                        # tokens.add(tok)
                                        if tok not in tokens:
                                            tokens.append(tok)
                                            if printDetails:
                                                print(tok)

    # return list(tokens)
    return tokens

def token_in_list_ignore_case(token, list_check):
    for tok in list_check:
        if tok.lower() == token.lower():
            return True
    return False

def token_notin_list_ignore_case(token, list_check):
    for tok in list_check:
        if tok.lower() == token.lower():
            return False
    return True

def column_to_be_ignored(columnName, specDict, delimiter='!!'):
    retValue = False
    if 'ignoreColumns' in specDict:
        for ignoreToken in specDict['ignoreColumns']:
            if delimiter in ignoreToken and set(ignoreToken.split(delimiter)).issubset(columnName.split(delimiter)):
                retValue = True
            elif token_in_list_ignore_case(ignoreToken, columnName.split(delimiter)):
                retValue = True
    return retValue

def remove_columns_to_be_ignored(columnNameList, specDict, delimiter='!!'):
    retList = []
    for columnName in columnNameList:
        if not column_to_be_ignored(columnName, specDict, delimiter):
            retList.append(columnName)
    return retList

def ignored_columns(columnNameList, specDict, delimiter='!!'):
    retList = []
    for columnName in columnNameList:
        if column_to_be_ignored(columnName, specDict, delimiter):
            retList.append(columnName)
    return retList

# assumes columnNameList does not contain columns to be ignored
def get_tokens_list_from_column_list(columnNameList, delimiter='!!'):
    # tokens = set()
    tokens = []
    for columnName in columnNameList:
        for tok in columnName.split(delimiter):
            # tokens.add(tok)
            if tok not in tokens:
                tokens.append(tok)

    # return list(tokens)
    return tokens

def get_spec_token_list(specDict, delimiter='!!'):
    retList = []
    repeatedList = []
    # check if the token appears in any of the pvs
    for prop in specDict['pvs'].keys():
        for token in specDict['pvs'][prop]:
            if token in retList and not token.startswith('_'):

                repeatedList.append(token)
            else:   
                retList.append(token)
    
    # check if the token appears in any of the population type
    if 'populationType' in specDict:
        for token in specDict['populationType'].keys():
            if token in retList and not token.startswith('_'):
                repeatedList.append(token)
            else:   
                retList.append(token)
    
    # check if the token appears in measurement
    if 'measurement' in specDict:
        for token in specDict['measurement'].keys():
            if token in retList and not token.startswith('_'):
                repeatedList.append(token)
            else:   
                retList.append(token)
    
    #check if the token is to be ignored
    if 'ignoreTokens' in specDict:
        for token in specDict['ignoreTokens']:
            if token in retList and not token.startswith('_'):
                repeatedList.append(token)
            else:   
                retList.append(token)
    
    #check if the column name appears as ignore column or if a token appears in ignoreColumns
    if 'ignoreColumns' in specDict:
        for token in specDict['ignoreColumns']:
            if token in retList and not token.startswith('_'):
                repeatedList.append(token)
            else:   
                retList.append(token)
    
    #check if the token appears on any side of the enumspecialisation
    if 'enumSpecializations' in specDict:
        for token in specDict['enumSpecializations'].keys():
            retList.append(token)
            retList.append(specDict['enumSpecializations'][token])
    
    #check if the total clomn is present and tokens in right side of denominator appear
    if 'denominators' in specDict:
        for column in specDict['denominators']:
            retList.append(column)
            for token in specDict['denominators'][column]:
                retList.append(token)

    return {'token_list':list(set(retList)), 'repeated_list':list(set(repeatedList))}

def get_spec_DCID_list(specDict):
    retList = []
    
    # check if the token appears in any of the pvs
    for prop in specDict['pvs'].keys():
        for token in specDict['pvs'][prop]:
            if '[' not in specDict['pvs'][prop][token]:
                retList.append(specDict['pvs'][prop][token])
    
    # check if the token appears in any of the population type
    if 'populationType' in specDict:
        for token in specDict['populationType'].keys():   
            retList.append(specDict['populationType'][token])
    
    # TODO check if the token appears in measurement
    # if 'measurement' in specDict:
        # for token in specDict['measurement'].keys():
            # retList.append(token)
    # TODO
    # inferredPV, universePV
    
    return list(set(retList))

def find_missing_tokens(tokenList, specDict, delimiter='!!'):
    specTokens = get_spec_token_list(specDict, delimiter)['token_list']
    tokensCopy = tokenList.copy()
    for token in tokenList:
        if token_in_list_ignore_case(token, specTokens):
            tokensCopy.remove(token)
    return tokensCopy

# assumes metadata file or data with overlays file
def columns_from_CSVreader(csvReader, isMetadataFile = False):
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
def columns_from_CSVfile(csvPath, isMetadataFile = False):
    csvPath = os.path.expanduser(csvPath)
    csvReader = csv.reader(open(csvPath, 'r'))
    allColumns = columns_from_CSVreader(csvReader, isMetadataFile)

    return allColumns

# assumes metadata file or data with overlays file
def columns_from_CSVfile_list(csvPathList, isMetadata = [False]):
    allColumns = []

    if len(isMetadata) < len(csvPathList):
        for i in range(0,(len(csvPathList)-len(isMetadata))):
            isMetadata.append(False)

    for i, curFile in enumerate(csvPathList):
        # create csv reader
        curFile = os.path.expanduser(curFile)
        csvReader = csv.reader(open(curFile, 'r'))
        curColumns = columns_from_CSVreader(csvReader, isMetadata[i])
        allColumns.extend(curColumns)

    allColumns = list(set(allColumns))

    return allColumns

# assumes metadata file or data with overlays file
def columns_from_zip_file(zipPath, checkMetadata = False):
    zipPath = os.path.expanduser(zipPath)
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
                    curColumns = columns_from_CSVreader(csvReader, checkMetadata)
                    allColumns.extend(curColumns)

    allColumns = list(set(allColumns))

    return allColumns


def get_spec_dict_from_path(specPath):
    specPath = os.path.expanduser(specPath)
    with open(specPath, 'r') as fp:
        specDict = json.load(fp)
    return specDict

def main(argv):
    if not FLAGS.spec_path:
        if FLAGS.ignore_columns:
            print('ERROR: Path to spec JSON required to ignore columns')
            return
    else:
        spec_dict = get_spec_dict_from_path(FLAGS.spec_path)

    all_columns = []
    print_columns = []
    if FLAGS.zip_path:
        all_columns = columns_from_zip_file(FLAGS.zip_path, FLAGS.is_metadata)
        if FLAGS.ignore_columns:
            print_columns = remove_columns_to_be_ignored(all_columns, spec_dict, FLAGS.delimiter)
        else:
            print_columns = all_columns
    elif FLAGS.csv_path:
        all_columns = columns_from_CSVfile(FLAGS.csv_path, FLAGS.is_metadata)
        if FLAGS.ignore_columns:
            print_columns = remove_columns_to_be_ignored(all_columns, spec_dict, FLAGS.delimiter)
        else:
            print_columns = all_columns
    elif FLAGS.csv_path_list:
        all_columns = columns_from_CSVfile_list(FLAGS.csv_path_list, [FLAGS.is_metadata])
        if FLAGS.ignore_columns:
            print_columns = remove_columns_to_be_ignored(all_columns, spec_dict, FLAGS.delimiter)
        else:
            print_columns = all_columns

    if FLAGS.get_tokens:
        print(json.dumps(get_tokens_list_from_column_list(print_columns, FLAGS.delimiter), indent=2))

    if FLAGS.get_columns:
        print(json.dumps(print_columns, indent=2))

    if FLAGS.get_ignored_columns:
        print(json.dumps(list(set(ignored_columns(all_columns, spec_dict, FLAGS.delimiter))), indent=2))

if __name__ == '__main__':
    flags.mark_flags_as_mutual_exclusive(['zip_path', 'csv_path', 'csv_path_list'], required=True)
    app.run(main)