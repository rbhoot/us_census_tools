from common_util import *
from functools import cmp_to_key


def find_columns_with_token(column_list, token, delimiter='!!'):
  ret_list = []
  for cur_column in column_list:
    if token_in_list_ignore_case(token, cur_column.split(delimiter)):
      ret_list.append(cur_column)
  return list(set((ret_list)))


def replace_token_in_column(cur_column, old_token, new_token, delimiter='!!'):
  return delimiter.join(
      [new_token if x == old_token else x for x in cur_column.split(delimiter)])


def replace_first_token_in_column(cur_column,
                                  old_token,
                                  new_token,
                                  delimiter='!!'):
  new_list = []
  temp_flag = True
  for x in cur_column.split(delimiter):
    if x == old_token and temp_flag:
      new_list.append(new_token)
      temp_flag = False
    else:
      new_list.append(x)

  return delimiter.join(new_list)


# replace token
def replace_token_in_column_list(column_list,
                                 old_token,
                                 new_token,
                                 delimiter='!!'):
  ret_list = []
  for cur_column in column_list:
    ret_list.append(
        replace_token_in_column(cur_column, old_token, new_token, delimiter))
  return ret_list


# combined replace list
def replace_token_list_in_column_list(column_list,
                                      old_token,
                                      new_token_list,
                                      delimiter='!!'):
  ret_dict = {}
  for cur_column in column_list:
    ret_dict[cur_column] = []
    for new_token in new_token_list:
      ret_dict[cur_column].append(
          replace_token_in_column(cur_column, old_token, new_token, delimiter))
  return ret_dict


# find columns sub token
def find_columns_with_token_partial_match(column_list,
                                          token_str,
                                          delimiter='!!'):
  ret_list = []
  for cur_column in column_list:
    for token in cur_column.split(delimiter):
      if token_str.lower() in token.lower():
        ret_list.append(cur_column)
  return list(set((ret_list)))


def get_columns_by_token_count(column_list, delimiter='!!'):
  ret_dict = {}
  for cur_column in column_list:
    token_list = cur_column.split(delimiter)
    if len(token_list) not in ret_dict:
      ret_dict[len(token_list)] = []
    ret_dict[len(token_list)].append(cur_column)
  return ret_dict


def get_columns_with_same_prefix(columns_by_length,
                                 max_extra_token=1,
                                 delimiter='!!'):
  ret_dict = {}
  for column_length in columns_by_length:
    for cur_column in columns_by_length[column_length]:
      extra_length = 1
      while extra_length <= max_extra_token:
        if (column_length + extra_length) in columns_by_length:
          for comapre_column in columns_by_length[column_length + extra_length]:
            if cur_column in comapre_column:
              if cur_column not in ret_dict:
                ret_dict[cur_column] = []
              ret_dict[cur_column].append(comapre_column)
        extra_length += 1
  return ret_dict


def guess_total_columns_from_zip_file(zip_path, ignore_token_list):
  zip_path = os.path.expanduser(zip_path)
  
  ret_dict = {}

  with zipfile.ZipFile(zip_path) as zf:
    for filename in zf.namelist():
      if '_data_' in filename:
        # find year
        year = filename[:filename.index('.')]
        year = year[-4:]
        with zf.open(filename, 'r') as data_f:
          csv_reader = csv.reader(io.TextIOWrapper(data_f, 'utf-8'))
          ret_dict[year] = total_columns_from_csvreader(csv_reader, ignore_token_list)
  
  return ret_dict

def total_columns_from_csvreader(csv_reader, ignore_token_list):
  total_columns = []
  for row in csv_reader:
    if csv_reader.line_num == 2:
      column_name_list = row.copy()
    elif csv_reader.line_num == 3:
      for i, val in enumerate(row):
        try:
          ignore_cell = False
          for tok in ignore_token_list:
            if tok.lower() in column_name_list[i].lower():
              ignore_cell = True
          if 'Margin of Error' in column_name_list[i]:
            ignore_cell = True
          if not ignore_cell:  
            if float(val) > 100:
              if column_name_list[i] not in total_columns:
                total_columns.append(column_name_list[i])
        except ValueError:
          pass
  return total_columns


def column_find_prefixed(column_name, prefix_list):
  matched_prefix = None
  if column_name not in prefix_list:
    for cur_prefix in prefix_list:
      if len(cur_prefix) < len(column_name) and cur_prefix in column_name:
        if matched_prefix:
          if len(cur_prefix) > len(matched_prefix):
            matched_prefix = cur_prefix
        else:
          matched_prefix = cur_prefix

  return matched_prefix

def get_census_column_token_index(census_columns, year_list, yearwise_columns, delimiter='!!'):
  index_dict = {}
  ret_dict = {}
  for year in yearwise_columns:
    if year in year_list:
      index_dict[year] = {}
    for census_col in census_columns:
      if year != 'all':
        index_dict[year][census_col] = {}
        index_dict[year][census_col]['index'] = []
  
  for year in yearwise_columns:
    if year in year_list:
      # compile list of column token index, traversing each row
      for census_cell in yearwise_columns[year]:
        token_list = census_cell.split(delimiter)
        column_found = False
        for census_col in census_columns:
          
          if census_col in token_list:# or census_col+' MOE' in token_list:
            column_found = True
            # find the token index of column name for each year
            col_i = token_list.index(census_col)
            if col_i not in index_dict[year][census_col]['index']:
              index_dict[year][census_col]['index'].append(col_i)
          # MOE column names
          if census_col+' MOE' in token_list:# or census_col+' MOE' in token_list:
            column_found = True
            # find the token index of column name for each year
            col_i = token_list.index(census_col+' MOE')
            if col_i not in index_dict[year][census_col]['index']:
              index_dict[year][census_col]['index'].append(col_i)
        if not column_found:
          print('Warning: No column found for', census_cell)
      
      # find the census column token index for the year
      year_col_i = -1
      for census_col in census_columns:
        # keep the lowest of the found indices, if multiple found
        index_dict[year][census_col]['index'] = sorted(index_dict[year][census_col]['index'])
        if year_col_i == -1:
          year_col_i = index_dict[year][census_col]['index'][0]
        # check if it is consistent across columns
        if year_col_i != index_dict[year][census_col]['index'][0]:
          print('Warning: found potential conflicts for column token index for year', year)
      
      ret_dict[year] = year_col_i
  
  ## For debug
  # print(json.dumps(index_dict, indent=2))
  return ret_dict

def get_census_rows_by_column(census_columns, year_list, yearwise_columns, index_dict, delimiter='!!'):
  # store the rows according to their columns
  ret_dict = {}
  for year in yearwise_columns:
    if year in year_list:
      ret_dict[year] = {}
      for census_col in census_columns:
        ret_dict[year][census_col] = []
      for census_cell in yearwise_columns[year]:
        token_list = census_cell.split(delimiter)
        for census_col in census_columns:
          if token_list[index_dict[year]] == census_col or token_list[index_dict[year]] == census_col+' MOE':
            if census_cell not in ret_dict[year][census_col]:
              ret_dict[year][census_col].append(census_cell)
  
  return ret_dict

def get_census_rows_by_column_by_type(rows_by_column, delimiter='!!'):
  # store the rows according to their columns and type
  ret_dict = {}
  for year in rows_by_column:
    ret_dict[year] = {}
    for census_col in rows_by_column[year]:
      ret_dict[year][census_col] = {
        'moe_cols': [],
        'estimate_cols': []
      }
      for census_cell in rows_by_column[year][census_col]:
        token_list = census_cell.split(delimiter)
        if 'Margin of Error' in token_list:
          ret_dict[year][census_col]['moe_cols'].append(census_cell)
        else:
          ret_dict[year][census_col]['estimate_cols'].append(census_cell)
  
  return ret_dict

def get_column_total_status(totals_by_column, rows_by_column_type, ignore_token_list):
  ret_dict = {}
  for year in totals_by_column:
    for census_col in totals_by_column[year]:
      if census_col not in ret_dict:
        ret_dict[census_col] = {}
      ret_dict[census_col][year] = { 'only_percentage': False,
        'only_total': False,
      }
      # only percentages
      if len(totals_by_column[year][census_col]) == 0:
        ret_dict[census_col][year]['only_percentage'] = True
      
      # only totals
      if len(totals_by_column[year][census_col]) == len(rows_by_column_type[year][census_col]['estimate_cols']):
        ret_dict[census_col][year]['only_total'] = True
  
  return ret_dict

def get_denominator_method_config(totals_status: dict, totals_by_column: dict) -> dict:
  ret_dict = {}
  ret_dict['denominator_method'] = ''
  total_columns = []
  percent_columns = []

  for census_col in totals_status:
    col_is_total = 1
    col_is_percent = 1
    for year in totals_status[census_col]:
      if col_is_total == 1:
        col_is_total = totals_status[census_col][year]['only_total']
      if col_is_total != totals_status[census_col][year]['only_total']:
        col_is_total = 2
      if col_is_percent == 1:
        col_is_percent = totals_status[census_col][year]['only_percentage']
      if col_is_percent != totals_status[census_col][year]['only_percentage']:
        col_is_percent = 2
    
    if col_is_percent == 2:
      ret_dict['denominator_method'] = 'year_mismatch'
    elif col_is_percent:
      percent_columns.append(census_col)
    if col_is_total == 2:
      ret_dict['denominator_method'] = 'year_mismatch'
    elif col_is_total:
      total_columns.append(census_col)

  if len(percent_columns) > 0 and len(total_columns) > 0:
    ret_dict['denominator_method'] = 'token_replace'
    ret_dict['token_map'] = {}
    for tok in percent_columns:
      ret_dict['token_map'][tok] = total_columns[0]
    if len(total_columns) > 1:
      print('Warning: The config might need fixing of token_map section because multiple total columns were found')
  # prefix method
  else:
    ret_dict['denominator_method'] = 'prefix'
    temp_dict = {'col': '', 'len': 0}
    len_dict = {}
    for year in totals_by_column:
      for census_col in totals_by_column[year]:
        # sanity checks
        if census_col not in len_dict:
          len_dict[census_col] = len(totals_by_column[year][census_col])
        elif len_dict[census_col] != len(totals_by_column[year][census_col]):
          print('Warning: number of totals for', census_col, 'changes across years, modify the long config if needed')

        # find longest list of totals to use, ideally should be same for all columns
        if len(totals_by_column[year][census_col]) > temp_dict['len']:
          temp_dict['col'] = census_col
          temp_dict['len'] = len(totals_by_column[year][census_col])
          temp_dict['rows'] = totals_by_column[year][census_col]
    
    ret_dict['reference_column'] = temp_dict['col']
    ret_dict['totals'] = temp_dict['rows']

  return ret_dict

# create config
def create_long_config(basic_config_path: str, delimiter: str = '!!'):
  basic_config_path = os.path.expanduser(basic_config_path)
  config_dict = json.load(open(basic_config_path))
  
  spec_dict = get_spec_dict_from_path(config_dict['spec_path'])

  us_data_zip = os.path.expanduser(config_dict['us_data_zip'])

  yearwise_columns_path = os.path.expanduser(config_dict['yearwise_columns_path'])
  yearwise_columns = json.load(open(yearwise_columns_path))

  census_columns = config_dict['census_columns']
  used_columns = config_dict['used_columns']
  year_list = config_dict['year_list']
  ignore_tokens = config_dict['ignore_tokens']
  
  
  for year in yearwise_columns:
    # remove ignoreColumns
    yearwise_columns[year] = remove_columns_to_be_ignored(yearwise_columns[year], spec_dict)
    # remove median, mean
    temp_list = []
    for column_name in yearwise_columns[year]:
      tok_found = False
      for tok in ignore_tokens:
        if tok in column_name:
          tok_found = True
      if not tok_found:
        temp_list.append(column_name)
    yearwise_columns[year] = temp_list
  
  yearwise_column_ind = get_census_column_token_index(census_columns, year_list, yearwise_columns, delimiter)
  # yearwise col_token index store in config
  config_dict['column_tok_index'] = yearwise_column_ind

  # find set all rows of each column yearwise
  yearwise_rows_by_column = get_census_rows_by_column(census_columns, year_list, yearwise_columns, yearwise_column_ind, delimiter)
  yearwise_rows_by_column_type = get_census_rows_by_column_by_type(yearwise_rows_by_column)

  # find possible totals
  yearwise_total_columns = guess_total_columns_from_zip_file(us_data_zip, ignore_tokens)
  for year in yearwise_total_columns:
    # remove ignoreColumns
    yearwise_total_columns[year] = remove_columns_to_be_ignored(yearwise_total_columns[year], spec_dict)

  # group by column name
  yearwise_totals_by_column = get_census_rows_by_column(used_columns, year_list, yearwise_total_columns, yearwise_column_ind, delimiter)
  
  yearwise_totals_status = get_column_total_status(yearwise_totals_by_column, yearwise_rows_by_column_type, ignore_tokens)

  temp_config = get_denominator_method_config(yearwise_totals_status, yearwise_totals_by_column)
  config_dict.update(temp_config)

  new_config_path = basic_config_path.replace('.json', '_long.json')
  json.dump(config_dict, open(new_config_path, 'w'), indent=2)

  # store yearwise_rows_by_column_type
  columns_path = new_config_path.replace('.json', '_columns.json')
  json.dump(yearwise_rows_by_column_type, open(columns_path, 'w'), indent=2)
  
  print(json.dumps(config_dict, indent=2))
  

def create_denominators_section(long_config_path: str, delimiter: str = '!!'):
  long_config_path = os.path.expanduser(long_config_path)
  config_dict = json.load(open(long_config_path))

  rows_by_column_type = json.load(open(long_config_path.replace('.json', '_columns.json')))
  denominators = {}

  if config_dict['denominator_method'] == 'token_replace':
    for new_col in config_dict['token_map']:
      total_col = config_dict['token_map'][new_col]
      for year in rows_by_column_type:
        col_i = config_dict['column_tok_index'][year]
        for new_total in rows_by_column_type[year][total_col]['estimate_cols']:
          if new_total not in denominators:
            denominators[new_total] = []
          # replace new_col in new_total
          temp_str = rename_col(new_total, new_col, col_i, delimiter)
          # check and add
          if temp_str in rows_by_column_type[year][new_col]['estimate_cols']:
            if temp_str not in denominators[new_total]:
              denominators[new_total].append(temp_str)
          else:
            print('Warning: column expected but not found\n', temp_str)
          
          # replace new_col and Margin of Error in new_total
          temp_str2 = replace_token_in_column(temp_str, 'Estimate',
                                           'Margin of Error', delimiter)
          # check and add
          if temp_str2 in rows_by_column_type[year][new_col]['moe_cols']:
            if temp_str2 not in denominators[new_total]:
              denominators[new_total].append(temp_str2)
          
          # replace new_col+ MOE and Margin of Error in new_total
          temp_str3 = rename_col(new_total, new_col+' MOE', col_i, delimiter)
          temp_str3 = replace_token_in_column(temp_str3, 'Estimate',
                                           'Margin of Error', delimiter)
          # check and add
          if temp_str3 in rows_by_column_type[year][new_col]['moe_cols']:
            if temp_str3 not in denominators[new_total]:
              denominators[new_total].append(temp_str3)

          if temp_str2 not in rows_by_column_type[year][new_col]['moe_cols'] and temp_str3 not in rows_by_column_type[year][new_col]['moe_cols']:
            print('Warning: column expected but not found\n', temp_str2, '\nor\n', temp_str3)
          
          

  # print(json.dumps(denominators, indent=2))
  json.dump(denominators, open('denominators.json', 'w'), indent=2)
  if config_dict['update_spec']:
    spec_dict = get_spec_dict_from_path(config_dict['spec_path'])
    spec_dict['denominators'] = denominators
    json.dump(spec_dict, open(config_dict['spec_path'], 'w'), indent=2)


def rename_col(row_name, new_col, col_i, delimiter='!!'):
  temp_list = row_name.split(delimiter)
  temp_list[col_i] = new_col
  temp_str = delimiter.join(temp_list)
  return temp_str
          

  

create_long_config('denominator_config_sample.json')
create_denominators_section('denominator_config_sample_long.json')
# create_long_config('denominator_config_S2408.json')

# # get column names
# # get column names to consider for percentage
# # if none, all would be used
# # find token index of column name
# # token name or token name + MOE
# # warn columns without token
# # intersection of all
# # guess method
# # column to column map
# # all the percent values match same token or token + MOE
# # prefix
# # all the percent values have a column that is non percent and can be prefixed
# # none
# # non prefix matching columns

# delimiter = '!!'
# # all_columns = columnsFromZipFile('~/acs_tables/S2603/S2603_us.zip')
# # spec_dict = getSpecDictFromPath('../spec_dir/S2603_spec.json')
# all_columns = columns_from_zip_file('~/acs_tables/S2602/S2602_us.zip')
# spec_dict = get_spec_dict_from_path('../spec_dir/S2602_spec.json')

# all_columns = remove_columns_to_be_ignored(all_columns, spec_dict)

# # print(json.dumps(getcolumnsByTokenCount(all_columns), indent=2))
# # print(json.dumps(getColumnsWithSamePrefix(getcolumnsByTokenCount(all_columns), 2), indent=2))

# # total_columns = guessTotalColumnsFromZipFile('~/acs_tables/S2603/S2603_us.zip')
# total_columns = guess_total_columns_from_zip_file(
#     '~/acs_tables/S2602/S2602_us.zip')

# # total_columns = removeColumnsToBeIgnored(total_columns, spec_dict)

# # print(json.dumps(total_columns, indent=2))

# # column_names = ['Total population', 'Total group quarters population', 'Adult correctional facilities', 'Nursing facilities/skilled nursing facilities', 'Juvenile Facilities', 'College/university housing', 'Military quarters/military ships']
# # replace_columns_names = ['Total population', 'Adult correctional facilities', 'Nursing facilities/skilled nursing facilities', 'Juvenile Facilities', 'College/university housing', 'Military quarters/military ships']
# column_names = [
#     'Total population', 'Total group quarters population',
#     'Adult correctional facilities',
#     'Nursing facilities/skilled nursing facilities',
#     'College/university housing'
# ]
# replace_columns_names = [
#     'Total population', 'Adult correctional facilities',
#     'Nursing facilities/skilled nursing facilities',
#     'College/university housing'
# ]

# total_columns = find_columns_with_token(total_columns,
#                                         'Total group quarters population')

# # replace with other groups to get total_columns
# for column_name in total_columns.copy():
#   # temp_str = replaceTokenInColumn(column_name, 'Estimate', 'Margin of Error', delimiter)
#   # if temp_str not in all_columns:
#   # 	print(temp_str)
#   # total_columns.append(temp_str)
#   for new_token in replace_columns_names:
#     other_column = replace_token_in_column(column_name,
#                                            'Total group quarters population',
#                                            new_token, delimiter)
#     total_columns.append(other_column)
#     # temp_str = replaceTokenInColumn(other_column, 'Estimate', 'Margin of Error', delimiter)
#     # total_columns.append(temp_str)
#     # if temp_str not in all_columns:
#     # 	print(temp_str)

# denominators = {}
# for cur_column in all_columns:
#   # TODO create other list of prefixes
#   if 'Median' not in cur_column and 'Mean' not in cur_column:
#     prefix = column_find_prefixed(cur_column, total_columns)
#     if prefix:
#       if prefix not in denominators:
#         denominators[prefix] = []
#       denominators[prefix].append(cur_column)
#       temp_str = replace_token_in_column(cur_column, 'Estimate',
#                                          'Margin of Error', delimiter)
#       if temp_str not in all_columns:
#         print('Column expected but not found:', temp_str)
#       for column_group in column_names:
#         temp_str = replace_token_in_column(cur_column, 'Estimate',
#                                            'Margin of Error', delimiter)
#         temp_str = replace_first_token_in_column(temp_str, column_group,
#                                                  column_group + ' MOE',
#                                                  delimiter)
#         if temp_str in all_columns and temp_str not in denominators[prefix]:
#           denominators[prefix].append(temp_str)
#         # if temp_str not in all_columns:
#         # print('Column expected but not found:', temp_str)
#         # print(temp_str)
#       if temp_str in all_columns and temp_str not in denominators[prefix]:
#         denominators[prefix].append(temp_str)
#       # if temp_str not in all_columns:
#       # print('Column expected but not found:', temp_str)
#       # print(temp_str)

# print(json.dumps(sorted(total_columns), indent=2))
# print(json.dumps(list(denominators.keys()), indent=2))
# print(json.dumps(dict(sorted(denominators.items())), indent=2))

# total_moe_columns = find_columns_with_token(all_columns, 'Margin of Error')
# median_moe_columns = find_columns_with_token_partial_match(
#     total_moe_columns, 'Median')
# mean_moe_columns = find_columns_with_token_partial_match(
#     total_moe_columns, 'Mean')

# print(
#     json.dumps(
#         list(set(median_moe_columns).union(set(mean_moe_columns))), indent=2))

# # add columns that have substring but not equal len(a) < len(b) and a in b
# # replace estimate with margin of error, substring
# # replace column name with column name + MOE, substring

# total_moe_columns = find_columns_with_token(total_columns, 'Margin of Error')
# median_columns = find_columns_with_token_partial_match(total_columns, 'Median')
# mean_columns = find_columns_with_token_partial_match(total_columns, 'Mean')

# only_totals = list(
#     set(total_columns) - set(total_moe_columns) - set(median_columns) -
#     set(mean_columns))

# # add columns that have substring but not equal len(a) < len(b) and a in b
# # replace estimate with margin of error,
# # replace column name with column name + MOE

# # print(json.dumps(sorted(only_totals, key=cmp_to_key(compare)), indent=2))

# all_columns = columns_from_zip_file('~/acs_tables/S0701PR/S0701PR_us.zip')
# spec_dict = get_spec_dict_from_path('../spec_dir/S0701PR_spec.json')
# ign_list = []
# for curColumn in all_columns:
#   if curColumn.endswith('!!MARITAL STATUS!!Population 15 years and over'):
#     ign_list.append(curColumn)
# print(json.dumps(ign_list, indent=2))

# for curColumn in all_columns:
#   if 'Population 15 years and over!!$75,000 or more' in curColumn and (
#       'Margin '
#       'of '
#       'Error') in curColumn and 'Total' in curColumn:
#     print(curColumn)
# moe_columns = find_columns_with_token(all_columns, 'Margin of Error')

# # get all columns

# all_columns = remove_columns_to_be_ignored(all_columns, spec_dict)
# total_columns = find_columns_with_token(all_columns, 'Total')
# moe_columns = find_columns_with_token(all_columns, 'Margin of Error')

# #remove median related columns
# median_columns = find_columns_with_token_partial_match(all_columns, 'Median')
# median_moe_columns = find_columns_with_token_partial_match(
#     median_columns, 'Margin of Error')
# # print(json.dumps(median_moe_columns, indent = 2))
# print(len(median_columns))
# total_columns = list(set(total_columns) - set(median_columns))
# total_columns = list(set(total_columns) - set(moe_columns))

# denominators_section = {}
# # total_replacements = ['Moved; within same county', 'Moved; from different county, same state', 'Moved; from different  state', 'Moved; from abroad']
# total_replacements = [
#     'Moved; within same municipio', 'Moved; from different municipio',
#     'Moved; from the U.S.', 'Moved; from outside Puerto Rico and the U.S.'
# ]

# # denominators_section = replaceTokenListInColumnList(total_columns, 'Total', ['Moved; within same county', 'Moved; from different county, same state', 'Moved; from different  state', 'Moved; from abroad'])
# for column_name in total_columns:
#   denominators_section[column_name] = []
#   for token in total_replacements:
#     new_column = replace_token_in_column(column_name, 'Total', token)
#     new_column_moe = replace_token_in_column(column_name, 'Total',
#                                              token + ' MOE')
#     if new_column in all_columns:
#       denominators_section[column_name].append(new_column)
#     temp_str = replace_token_in_column(new_column, 'Estimate',
#                                        'Margin of Error')
#     if temp_str in all_columns:
#       denominators_section[column_name].append(temp_str)
#     temp_str = replace_token_in_column(new_column_moe, 'Estimate',
#                                        'Margin of Error')
#     if temp_str in all_columns:
#       denominators_section[column_name].append(temp_str)
# # denominators_section = replaceTokenListInColumnList(total_columns, 'Total', ['Moved; within same municipio', 'Moved; from different municipio', 'Moved; from the U.S.', 'Moved; from outside Puerto Rico and the U.S.'])

# print(json.dumps(denominators_section, indent=2))

# total_moe_columns = find_columns_with_token(all_columns, 'Total MOE')
# median_moe_columns = find_columns_with_token_partial_match(
#     total_moe_columns, 'Median')
# print(len(median_moe_columns))
# print(json.dumps(median_moe_columns, indent=2))
# total_moe_columns = list(set(total_moe_columns) - set(median_moe_columns))
# # denominators_section.update(replaceTokenListInColumnList(total_moe_columns, 'Total MOE', ['Moved; within same county MOE', 'Moved; from different county, same state MOE', 'Moved; from different  state MOE', 'Moved; from abroad MOE']))
# denominators_section.update(
#     replace_token_list_in_column_list(total_moe_columns, 'Total MOE', [
#         'Moved; within same municipio MOE',
#         'Moved; from different municipio MOE', 'Moved; from the U.S. MOE',
#         'Moved; from outside Puerto Rico and the U.S. MOE'
#     ]))

# print(json.dumps(denominators_section, indent=2))

# median_moe = find_columns_with_token(median_columns, 'Margin of Error')
# median_est = find_columns_with_token(median_columns, 'Estimate')
# replaced_columns = replace_token_in_column_list(median_est, 'Estimate',
#                                                 'Margin of Error')
# print('margin', len(find_columns_with_token(median_columns, 'Margin of Error')))
# print('estimate', len(find_columns_with_token(median_columns, 'Estimate')))
# print(len(find_columns_with_token_partial_match(total_moe_columns, 'Median')))
# print(list(set(median_moe) - set(replaced_columns)))

# print(list(set(total_columns) - set(median_columns)))
# total_columns = list(set(total_columns) - set(median_columns))
# replaced_columns = replace_token_in_column_list(total_columns, 'Estimate',
#                                                 'Margin of Error')
# moe_columns = find_columns_with_token(all_columns, 'Margin of Error')

# print(json.dumps(total_columns, indent=1))
# print(json.dumps(list(set(replaced_columns) - set(moe_columns)), indent=2))

# print(len(total_moe_columns))
# print(len(find_columns_with_token(median_columns, 'Margin of Error')))
# print(len(find_columns_with_token_partial_match(all_columns, 'Median')))
