from acs_spec_compiler import *

universal_spec_out = create_combined_spec(get_spec_list('../spec_dir/'))

create_new_spec(['~/Downloads/S0701.zip'], universal_spec_out, expected_populations=['Person'], expected_pvs=[])

# expected_pvs = ['age', 'gender', 'race', 'nativity', 'citizenship', 'maritalStatus', 'residentStatus', 'educationalAttainment', 'income', 'povertyStatus', 'occupancyTenure']
# create_new_spec('~/Downloads/S0701.zip', universal_spec_out, expected_populations=['Person'], expected_pvs=expected_pvs)