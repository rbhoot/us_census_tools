WARNING:
Save a copy of the generated spec if it is modified. Running the script again will overwrite the changes

Check the missing_report.json file for list of columns that need attention

The script creates a 'universal spec' which contains everythin from all the specs present in the spec_dir

It then proceeds to split it in 2 parts:
- keeping the parts where token match occours
- storing the discarded parts in other similar spec for reference in case of similar words