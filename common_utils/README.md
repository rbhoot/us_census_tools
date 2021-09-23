Common utils has a set of functionalities that can be used for any basic US census file dealing

It offers following functionalities through similar named functions:
- Get Token List from a ZIP file
- Get list of columns from a csv reader object
- Get list of columns from a csv file path
- Get list of columns from a csv file path list
- Get list of columns from a zip file
- Get python dictionary object from Config Spec JSON path
- Check if column needs to be ignored
- Remove the columns that need to be ignored from a list of columns
- Get a list of ignored columns from a list of columns
- Get a list of tokens from a list of columns
- Find tokens missing in a spec
- Get request a URL and get JSOn object from the response body

## Command Line Invocations

Use one of the following 3 flags to provide input csv files:
- zip_path
- csv_path
- csv_path_list

use spec_path flag to input the JSON spec

functionalities:
get_tokens: this flag produces the list of all unique tokens in the dataset
get_columns: this flag produces all the unique columns in the dataset
get_ignored_columns: this flag produces a list of all columns that were ignored according to the ignoreColumns section of the spec, NOTE: spec_path flag is necessary for this to work
ignore_columns: this flag allows restricting the outputs to have values only from the columns that were not ignored

additional flags for parsing configuration:
delimiter: By default this is set tp '!!'
is_metadata: this flag can be used to process only metadata files in case the data files are big

## Command Line Examples

To get list of all tokens:
```
python common_util.py --get_tokens --zip_path=../sample_data/s1810.zip
```

To get list of all tokens after ignored columns have been removed:
```
python common_util.py --get_tokens --ignore_columns --zip_path=../sample_data/s1810.zip --spec_path=../spec_dir/S1810_spec.json
```
Replace get_tokens with get_columns to get list of columns

To get the list of ignored columns:
```
python common_util.py --get_ignored_columns --zip_path=../sample_data/s1810.zip --spec_path=../spec_dir/S1810_spec.json
```



