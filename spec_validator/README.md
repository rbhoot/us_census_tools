The validator tool tries to do basic sanity checks on the spec.

The script parses:
- The set of CSV files downloaded from census
	- Single CSV file path
	- List of CSV file paths
	- ZIP file path
- Config Spec JSON file

It tries to identify:
- List and count of all column names
- List and count of all ignored column names
- List and count of all accepted column names
- Tokens appearing in spec but not in any of the CSV files
- Tokens appearing in CSV files but not in spec
- Columns that have no property assignment
- Columns that might possibly be in conflict with ignore and property assignment
- Missing Denominator Total Columns
- Possible missing EnumSpecialisations

The script generates column and token data file and another file with outcome of results in the output directory

'all' key of dictionary refers to combination across all the files in the input

filename vased outcomes can also be found in the output files

'filewise' option is generally used for debug purposes only

'checkMetadata' for zip file, 'isMetadata' for CSV files can be made True if _metadata_ type files need to be processed

check validate_spec.py for example invokations
