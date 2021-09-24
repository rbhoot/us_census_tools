**WARNING:**
Save a copy of the generated spec if it is modified. Running the script again will overwrite the changes

Check the missing_report.json file for list of columns that need attention

The script creates a 'compiled spec' which contains everythin from all the specs present in the spec_dir

It then proceeds to split 'compiled spec' in 2 parts:
- keeping the parts where token match occours
- storing the discarded parts in other similar spec for reference in case of similar token

## Command Line Invocations

To generate a guess spec:
```
python acs_spec_compiler.py --guess_new_spec --zip_path=../sample_data/s1810.zip
```
NOTE: This command creates following important files to lookout for:
- generate_spec.json: This is the guessed spec for the input file
- missing_report.json: This file contains:
	- List of tokens present in the dataset but not in the spec
	- List of columns that were not assigned any propery and value
- union_spec.json: This is the union of all the 
- discarded_spec_parts.json: This contains parts of the union spec that were not used in the output spec

To generate a guess spec with expected properties or population types:
```
python acs_spec_compiler.py --guess_new_spec --zip_path=../sample_data/s1810.zip --expected_populations=Person,Household --expected_properties=occupancyTenure
```
This will look for properties on DataCommons API and add placeholders for available enum values


To create a union of all specs:
```
python acs_spec_compiler.py --create_union_spec
```
NOTE: The output is also stored in file 'union_spec.json'

If the specs are present in some other directory:
```
python acs_spec_compiler.py --create_union_spec --spec_dir=<path to dir>
```

To get a list of properties available in the union of all specs:
```
python acs_spec_compiler.py --get_combined_property_list
```
Other available flags from common_utils:
- is_metadata
- delimiter

Refer to README in the common_utils folder for description
