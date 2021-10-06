import json
import os

table_id = 'S0701'
table_dir = os.path.expanduser(f'~/Documents/acs_tables/{table_id}/')
column_map_path = os.path.join(table_dir, 'column_map.json')

os.path.expanduser(column_map_path)
column_map = json.load(open(column_map_path, 'r'))

stat_dir = {}
dcid_list_all = {}
for year in column_map:
	stat_dir[year] = {}
	stat_dir[year]['actual_column_count'] = len(column_map[year])
	
	moe_stats_count = 0
	estimate_stat_count = 0
	dcid_list = {}
	
	# TODO column should have been ignored
	
	for column in column_map[year]:
		if column_map[year][column]['Node'] not in dcid_list:
			dcid_list[column_map[year][column]['Node']] = []
		dcid_list[column_map[year][column]['Node']].append(column)
		# margin of error and normal statvar counts
		if column_map[year][column]['statType'] == "dcid:marginOfError":
			moe_stats_count += 1
		else:
			estimate_stat_count += 1
	
	dcid_list_all[year] = dcid_list
	
	stat_dir[year]['estimate_count'] = estimate_stat_count
	stat_dir[year]['moe_count'] = moe_stats_count
	# stat_dir[year]['dcid_list'] = dcid_list
	stat_dir[year]['repeated_dcids'] = {}
	for dcid in dcid_list:
		if len(dcid_list[dcid]) > 1:
			stat_dir[year]['repeated_dcids'].update({dcid: dcid_list[dcid]})
	
	# TODO missing columns in output


print(json.dumps(list(set(dcid_list_all['2016'])-set(dcid_list_all['2019'])), indent=2))


json.dump(stat_dir, open(os.path.join(table_dir+'column_map_stats.json'), 'w'), indent=2)
# print(json.dumps(stat_dir, indent=2))