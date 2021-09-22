from acs_api import *

api_key = "YOUR_KEY_HERE"

download_table('S1810', list(range(2013, 2020)), 'geoURLMap_basic.json', 's1810/', api_key)
# use this if you just want to create a combined file of predownloaded data folder
# consolidate_files('S1810', list(range(2013, 2020)), 'data3/')