Common utils has a set of functionalities that can be used for any basic US census file dealing

# Functionalities

## common_util.py
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
- Get request a URL and get JSON object from the response body

## data_validations.py
This file takes as input the cleaned csv file generated from data loader.

The checks performed include:
- Check if country level data contains values which are less tha equal to 100. This test can be used to find columns that might have been missed be denominator section.
- Check and fix median like values with open bounds, e.g. Median Income 2500+

## helper_functions.py
This file can be used from command line to generate denominator section. The file also contains related functions which might be used elsewhere.

### Denominator Section Generation Process
Denominator section generation takes as input a config file of the format *denominator_config_sample.json*. 

The script works in 2 major steps:
- Creating a long config file from the config file and inputs linked to in config file. This step generates 2 new files that will be used for the next step:
  - `_long.json`
  - `_long_columns.json`
- Creating the denominators section using the long config file generated from previous step.

The long config file generated in the 1st step can be used to check and modify the denominator section generation process.

### Config File Options

- `spec_path` - The path to the JSON spec
- `us_data_zip` - The zip file containing **ONLY US level** data downloaded from census.
- `yearwise_columns_path` - Path of the file for the relevant table from clone of https://github.com/rbhoot/acs_tables .
- `update_spec` - Boolean value. If set to `true` the spec pointed by `spec_path` will be modified and the denominator section will be added/overwritten in the file.
- `ignore_tokens` - List of tokens which will dicard the column if any of the token appears even as substring of token. This is useful if some values behave like percentage but are true values and need not be considered for denominator section. Minimum list: ['Mean', 'Median']

**NOTE: The substring listed in this(ignore_tokens) section will be matched against entire column name, not just entire tokens. This can lead to token substring match too.**
- `census_columns` - The list of top level columns in census tables. Refer to sample file for example. This is used for finding the index of the token representing the census column name within the column name. NOTE: Do not add same column names suffixed by MOE. e.g. *Total* and *Total MOE*.
- `used_columns` - Subset of `census_columns` that are being used in the import. Most of the cases will have the 2 sections same. NOTE: **Do not** add same column names suffixed by MOE. e.g. *Total* and *Total MOE*.
- `year_list` - List of years for which denominator computation will be required.

### Long Config File Options
- `column_tok_index` - This contains the index of the token representing the census column name for each year.
- `denominator_method` - The method to be used for generating denominator section. It can be either of the following:
  - `token_replace` - This method is used when entire census column is a percentage value and the total is present in some other census column. This relationship is represented by:
    - `token_map` - The census column name containing percentage values and the corresponding census column containing it's total.
  - `prefix` - This method is used when each census column contains mixed values, percentage and totals both. The usual representation method is such that the column name containing the total value is present as a prefix in the column name containing percentage. **NOTE: If this convention is not followed, the generated denominators section will be incomplete. Warnings will be raised about such columns and they need to handled manually.**
    - `reference_column` - This is the census column used as refernce and all the totals appearing for this column will be replicated for all census columns under `used_columns`.
    - `totals` - Yearwise list of column names containing total values and hence the list of prefix matching used. The same will be used for other census columns listed under `used_columns`. It is a good idea to check this list against actual table, and tally the number of entries against it.

**NOTE: `prefix` method might fail if census doesn't use a prefix in the column name and mentions just the entity.**
    
### Common Guidelines For Using Denominator Generator
- Sometimes using a subset of years would lead to change in `denominator_method`. This might be the case when table format changes across years.
- Changing long config manually is an option and might be useful in certain scenarios like:
  - Table having multiple census columns representing totals. In such a case the first one is always picked. Changing the `token_map` section would be necessary to map the columns correctly.
  - Some prefix might have been missed, and might need to be added manually.
- In case wrong `denominator_method` is generated, please file a bug, the code logic might need some change.

# Command Line Invocations
## common_util.py
Use one of the following 3 flags to provide input csv files:
- `zip_path`
- `csv_path`
- `csv_list`

use `spec_path` flag to input the JSON spec

functionality flags:

- `get_tokens`: this flag produces the list of all unique tokens in the dataset

- `get_columns`: this flag produces all the unique columns in the dataset

- `get_ignored_columns`: this flag produces a list of all columns that were ignored according to the ignoreColumns section of the spec. NOTE: spec_path flag is necessary for this to work

- `ignore_columns`: this flag allows restricting the outputs to have values only from the columns that were not ignored.  NOTE: spec_path flag is necessary for this to work

additional flags for parsing configuration:
- `delimiter`: By default this is set tp '!!'
- `is_metadata`: this flag can be used to process only metadata files in case the data files are big

## data_validations.py
- `csv_path`: Path to the cleaned csv file, generated by the data loader or process script
- `quantity_tag`: The column name in the csv header of the column containing StatVar observation value. Default - *Quantity*
- `geo_tag`: The column name in the csv header of the column containing location of StatVar observation. Default - *Place*
- `statvar_tag`: The column name in the csv header of the column containing dcid of the StatVar. Default - *StatVar*
- `data_tests`: Can take following values - 
  - all
  - open_distributions
  - possible_percentage

## helper_functions.py
- `denominator_config`: The config file created by user using the sample file.
- `denominator_long_config`: The long config file generated from the confing file at the end of the first step, usually the same as first `denominator_config` with `_long` suffix in filename.

# Command Line Examples
## common_util.py
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

## data_validations.py

To fix values having open distributions:
```
python data_validations.py --data_tests=open_distributions --csv_path=~/acs_tables/S0701/run1/S0701_cleaned.csv
```
The above command generates 2 different csv files in the same directory:
- file name suffixed with `_fixed`.csv. This file is the usable new file.
- file name suffixed with `_fix_log`.csv. This file contains the initial versions of the rows that were modified. This file would be useful for future fix.

To find leftover percentage values:
```
python data_validations.py --data_tests=possible_percentage --csv_path=~/acs_tables/S0701/run1/S0701_cleaned.csv
```
The above command generates log csv files in the same directory with file name suffixed by `_possible_percentages`.csv. The log file contains list of all rows taht could possibly have percentage values rather than totals in output.

## helper_functions.py

To excute the 1st step of the denominator section generation:
```
python helper_functions.py --denominator_config=~/acs_tables/S0701/denominator_config.json
```
The above command generates 2 files in the same directory as config file that would be used by the 2nd step:
- long config file with filename suffixed with `_long`
- a column grouping json file with siffix `_long_columns`

To execute the 2nd step of denominator generation:
```
python helper_functions.py --denominator_long_config=~/acs_tables/S0701/denominator_config_long.json
```
The above command would create a denominators.json in the same directory as config file. If the flag has been set in config, the spec will be updated with generate4d denominators section.

In case no modification is needed in long config file:
```
python helper_functions.py --denominator_config=~/acs_tables/S0701/denominator_config.json --denominator_long_config=~/acs_tables/S0701/denominator_config_long.json
```
The above command will execute both the steps and create denominators.json file in the directory with config file.