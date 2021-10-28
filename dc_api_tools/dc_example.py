import dc_utils

print('------------------------------')
print('Person')
print('------------------------------')
print(dc_utils.fetch_dcid_properties_enums('Person'))
print('------------------------------')
print('Household')
print('------------------------------')
print(dc_utils.fetch_dcid_properties_enums('Household', force_fetch=True))
