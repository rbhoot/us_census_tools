import json
import os
import sys
from absl import app
from absl import flags

module_dir_ = os.path.dirname(__file__)
sys.path.append(os.path.join(module_dir_, '..'))

from common_utils.common_util import *

FLAGS = flags.FLAGS

flags.DEFINE_string('column_map', None,
                    'Path of json file containing the column StatVar map')
flags.DEFINE_string('yearwise_columns', None,
                    'Path of json file containing list of all columns by year')
flags.DEFINE_string('colmap_validation_output', '../output/',
                    'Directory path to write output file')


def check_column_map(column_map_path,
                     column_list_path,
                     spec_path,
                     output_path='../output/',
                     delimiter='!!'):
  column_map_path = os.path.expanduser(column_map_path)
  column_map = json.load(open(column_map_path, 'r'))

  column_list_path = os.path.expanduser(column_list_path)
  column_year_list = json.load(open(column_list_path, 'r'))

  spec_path = os.path.expanduser(spec_path)
  spec_dict = get_spec_dict_from_path(spec_path)

  output_path = os.path.expanduser(output_path)

  for year in column_year_list:
    temp_list = remove_columns_to_be_ignored(column_year_list[year], spec_dict,
                                             delimiter)
    column_year_list[year] = temp_list.copy()

  stat_dir = {}
  dcid_list_all = {}
  dcid_list_all['all'] = []
  cur_column_list = {}
  cur_column_list['all'] = []
  dcid_stat_var = {}

  stat_dir['same_dcid_different_statvar'] = []

  for year in column_map:
    stat_dir[year] = {}
    cur_column_list[year] = []
    stat_dir[year]['actual_column_count'] = len(column_map[year])
    stat_dir[year]['moe_only'] = []

    moe_stats_count = 0
    estimate_stat_count = 0
    dcid_list = {}

    for column_name in column_map[year]:
      cur_column_list[year].append(column_name)
      cur_dcid = column_map[year][column_name]['Node']
      if cur_dcid not in dcid_list:
        dcid_list[cur_dcid] = []
      dcid_list[cur_dcid].append(column_name)
      # margin of error and normal statvar counts
      if column_map[year][column_name]['statType'] == 'dcid:marginOfError':
        moe_stats_count += 1

        # check if the corresponding non margin of error statvar is present
        cmp_statvar = column_map[year][column_name].copy()
        cmp_statvar.pop('Node')
        cmp_statvar.pop('statType')

        temp_flag = True
        for column_name2, stat_var in column_map[year].items():
          if stat_var['statType'] != 'dcid:marginOfError':
            temp_statvar = stat_var.copy()
            temp_statvar.pop('Node')
            temp_statvar.pop('statType')
            if temp_statvar == cmp_statvar:
              temp_flag = False
        if temp_flag:
          stat_dir[year]['moe_only'].append(column_name)
      else:
        estimate_stat_count += 1

      if cur_dcid not in dcid_stat_var:
        dcid_stat_var[cur_dcid] = column_map[year][column_name]
      elif dcid_stat_var[cur_dcid] != column_map[year][column_name]:
        if cur_dcid not in stat_dir['same_dcid_different_statvar']:
          stat_dir['same_dcid_different_statvar'].append(cur_dcid)

    cur_column_list['all'].extend(cur_column_list[year])

    dcid_list_all[year] = list(dcid_list.keys())
    dcid_list_all['all'].extend(dcid_list_all[year])

    stat_dir[year]['estimate_count'] = estimate_stat_count
    stat_dir[year]['moe_count'] = moe_stats_count
    # stat_dir[year]['dcid_list'] = dcid_list
    stat_dir[year]['repeated_dcids'] = {}
    for dcid in dcid_list:
      if len(dcid_list[dcid]) > 1:
        stat_dir[year]['repeated_dcids'].update({dcid: dcid_list[dcid]})

    if stat_dir[year]['repeated_dcids']:
      print('Found some repeated dcids for year', year,
            ', please check output file')
    else:
      print('No repeated dcids for year', year)

    # missing columns in output
    missing_columns = list(
        set(column_year_list[year]) - set(cur_column_list[year]))

    if len(missing_columns) > 0:
      print('Found some columns missing for year', year,
            ', please check output file')
      stat_dir[year]['missing_columns'] = missing_columns
    else:
      print('No missing columns for year', year)

    # column should have been ignored
    extra_columns = list(
        set(cur_column_list[year]) - set(column_year_list[year]))

    if len(extra_columns) > 0:
      print('Found some extra columns for year', year,
            ', please check output file')
      stat_dir[year]['extra_columns'] = extra_columns
    else:
      print('No extra columns for year', year)

  cur_column_list['all'] = list(set(cur_column_list['all']))
  dcid_list_all['all'] = list(set(dcid_list_all['all']))

  # missing columns in output
  missing_columns = list(
      set(column_year_list['all']) - set(cur_column_list['all']))

  stat_dir['all'] = {}
  if len(missing_columns) > 0:
    stat_dir['all']['missing_columns'] = missing_columns

  # column should have been ignored
  extra_columns = list(
      set(cur_column_list['all']) - set(column_year_list['all']))

  if len(extra_columns) > 0:
    stat_dir['all']['extra_columns'] = extra_columns

  year_list = list(dcid_list_all.keys())
  year_list.remove('all')
  year_list = sorted(year_list)

  dcid_year_list = {}

  # dcid not in year
  for year in dcid_list_all:
    if year != 'all':
      year_missing = list(set(dcid_list_all['all']) - set(dcid_list_all[year]))
      if len(year_missing) > 0:
        stat_dir[year]['dcid_missing_in_year'] = year_missing.copy()
      # dcid only in that year
      temp_list = []
      for year2 in dcid_list_all:
        if year2 != 'all' and year2 != year:
          temp_list.extend(dcid_list_all[year2])

      unique_dcid = list(set(dcid_list_all[year]) - set(temp_list))
      if len(year_missing) > 0:
        stat_dir[year]['year_unique_dcid'] = unique_dcid.copy()
      for dcid in dcid_list_all[year]:
        if dcid not in dcid_year_list:
          dcid_year_list[dcid] = []
        if year not in dcid_year_list[dcid]:
          dcid_year_list[dcid].append(year)

  stat_dir['dcid_series_holes'] = {}
  for dcid in dcid_year_list:
    if sorted(dcid_year_list[dcid]) != year_list:
      stat_dir['dcid_series_holes'][dcid] = dcid_year_list[dcid]

  if stat_dir['dcid_series_holes']:
    print(
        'Found some dcids missing for some years, please check the output file')
  else:
    print('All dcids found across all years')

  print('Writing output file at', output_path)
  json.dump(
      stat_dir,
      open(os.path.join(output_path, 'column_map_validation.json'), 'w'),
      indent=2)
  # print(json.dumps(stat_dir, indent=2))


def main(argv):
  check_column_map(FLAGS.column_map, FLAGS.yearwise_columns, FLAGS.spec_path,
                   FLAGS.colmap_validation_output, FLAGS.delimiter)


if __name__ == '__main__':
  flags.mark_flags_as_required(['spec_path', 'column_map', 'yearwise_columns'])
  app.run(main)
