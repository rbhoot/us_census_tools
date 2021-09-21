This util does the work in 3 parts and each part produces a output file in the output folder
- fetch all the properties associated with the dcid (i.e. all the dcids that have domainIncludes = input dcid)
- fetch the type of the each property (i.e. list of rangeIncludes of each property fetched from the above step)
- fetch the enum of the each property if any of the rangeIncludes dcid contained the word enum (i.e. list of dcids that have typeOf as the enum name)

See the dc_example.py for use

The 'force_fetch' option forces an API call rather than creating output from prefetched files from previous runs