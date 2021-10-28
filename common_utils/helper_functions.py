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


def guess_total_columns_from_zip_file(zip_path):
  zip_path = os.path.expanduser(zip_path)
  total_columns = []

  with zipfile.ZipFile(zip_path) as zf:
    for filename in zf.namelist():
      if '_data_' in filename:
        with zf.open(filename, 'r') as data_f:
          csv_reader = csv.reader(io.TextIOWrapper(data_f, 'utf-8'))
          for row in csv_reader:
            if csv_reader.line_num == 2:
              column_name_list = row.copy()
            elif csv_reader.line_num == 3:
              for i, val in enumerate(row):
                try:
                  if float(val) > 100:
                    if 'Margin of Error' not in column_name_list[
                        i] and 'Median' not in column_name_list[
                            i] and 'Mean' not in column_name_list[
                                i] and 'INCOME' not in column_name_list[i]:
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


delimiter = '!!'
# all_columns = columnsFromZipFile('~/acs_tables/S2603/S2603_us.zip')
# spec_dict = getSpecDictFromPath('../spec_dir/S2603_spec.json')
all_columns = columns_from_zip_file('~/acs_tables/S2602/S2602_us.zip')
spec_dict = get_spec_dict_from_path('../spec_dir/S2602_spec.json')

all_columns = remove_columns_to_be_ignored(all_columns, spec_dict)

# print(json.dumps(getcolumnsByTokenCount(all_columns), indent=2))
# print(json.dumps(getColumnsWithSamePrefix(getcolumnsByTokenCount(all_columns), 2), indent=2))

# total_columns = guessTotalColumnsFromZipFile('~/acs_tables/S2603/S2603_us.zip')
total_columns = guess_total_columns_from_zip_file(
    '~/acs_tables/S2602/S2602_us.zip')

# total_columns = removeColumnsToBeIgnored(total_columns, spec_dict)

# print(json.dumps(total_columns, indent=2))

# column_names = ['Total population', 'Total group quarters population', 'Adult correctional facilities', 'Nursing facilities/skilled nursing facilities', 'Juvenile Facilities', 'College/university housing', 'Military quarters/military ships']
# replace_columns_names = ['Total population', 'Adult correctional facilities', 'Nursing facilities/skilled nursing facilities', 'Juvenile Facilities', 'College/university housing', 'Military quarters/military ships']
column_names = [
    'Total population', 'Total group quarters population',
    'Adult correctional facilities',
    'Nursing facilities/skilled nursing facilities',
    'College/university housing'
]
replace_columns_names = [
    'Total population', 'Adult correctional facilities',
    'Nursing facilities/skilled nursing facilities',
    'College/university housing'
]

total_columns = find_columns_with_token(total_columns,
                                        'Total group quarters population')

# get column names
# get column names to consider for percentage
# if none, all would be used
# find token index of column name
# token name or token name + MOE
# warn columns without token
# intersection of all
# guess method
# column to column map
# all the percent values match same token or token + MOE
# prefix
# all the percent values have a column that is non percent and can be prefixed
# none
# non prefix matching columns

# replace with other groups to get total_columns
for column_name in total_columns.copy():
  # temp_str = replaceTokenInColumn(column_name, 'Estimate', 'Margin of Error', delimiter)
  # if temp_str not in all_columns:
  # 	print(temp_str)
  # total_columns.append(temp_str)
  for new_token in replace_columns_names:
    other_column = replace_token_in_column(column_name,
                                           'Total group quarters population',
                                           new_token, delimiter)
    total_columns.append(other_column)
    # temp_str = replaceTokenInColumn(other_column, 'Estimate', 'Margin of Error', delimiter)
    # total_columns.append(temp_str)
    # if temp_str not in all_columns:
    # 	print(temp_str)

denominators = {}
for cur_column in all_columns:
  # TODO create other list of prefixes
  if 'Median' not in cur_column and 'Mean' not in cur_column:
    prefix = column_find_prefixed(cur_column, total_columns)
    if prefix:
      if prefix not in denominators:
        denominators[prefix] = []
      denominators[prefix].append(cur_column)
      temp_str = replace_token_in_column(cur_column, 'Estimate',
                                         'Margin of Error', delimiter)
      if temp_str not in all_columns:
        print('Column expected but not found:', temp_str)
      for column_group in column_names:
        temp_str = replace_token_in_column(cur_column, 'Estimate',
                                           'Margin of Error', delimiter)
        temp_str = replace_first_token_in_column(temp_str, column_group,
                                                 column_group + ' MOE',
                                                 delimiter)
        if temp_str in all_columns and temp_str not in denominators[prefix]:
          denominators[prefix].append(temp_str)
        # if temp_str not in all_columns:
        # print('Column expected but not found:', temp_str)
        # print(temp_str)
      if temp_str in all_columns and temp_str not in denominators[prefix]:
        denominators[prefix].append(temp_str)
      # if temp_str not in all_columns:
      # print('Column expected but not found:', temp_str)
      # print(temp_str)

print(json.dumps(sorted(total_columns), indent=2))
print(json.dumps(list(denominators.keys()), indent=2))
print(json.dumps(dict(sorted(denominators.items())), indent=2))

total_moe_columns = find_columns_with_token(all_columns, 'Margin of Error')
median_moe_columns = find_columns_with_token_partial_match(
    total_moe_columns, 'Median')
mean_moe_columns = find_columns_with_token_partial_match(
    total_moe_columns, 'Mean')

print(
    json.dumps(
        list(set(median_moe_columns).union(set(mean_moe_columns))), indent=2))

# add columns that have substring but not equal len(a) < len(b) and a in b
# replace estimate with margin of error, substring
# replace column name with column name + MOE, substring

total_moe_columns = find_columns_with_token(total_columns, 'Margin of Error')
median_columns = find_columns_with_token_partial_match(total_columns, 'Median')
mean_columns = find_columns_with_token_partial_match(total_columns, 'Mean')

only_totals = list(
    set(total_columns) - set(total_moe_columns) - set(median_columns) -
    set(mean_columns))

# add columns that have substring but not equal len(a) < len(b) and a in b
# replace estimate with margin of error,
# replace column name with column name + MOE

# print(json.dumps(sorted(only_totals, key=cmp_to_key(compare)), indent=2))

all_columns = columns_from_zip_file('~/acs_tables/S0701PR/S0701PR_us.zip')
spec_dict = get_spec_dict_from_path('../spec_dir/S0701PR_spec.json')
ign_list = []
for curColumn in all_columns:
  if curColumn.endswith('!!MARITAL STATUS!!Population 15 years and over'):
    ign_list.append(curColumn)
print(json.dumps(ign_list, indent=2))

for curColumn in all_columns:
  if 'Population 15 years and over!!$75,000 or more' in curColumn and ('Margin '
                                                                       'of '
                                                                       'Error') in curColumn and 'Total' in curColumn:
    print(curColumn)
moe_columns = find_columns_with_token(all_columns, 'Margin of Error')

# get all columns

all_columns = remove_columns_to_be_ignored(all_columns, spec_dict)
total_columns = find_columns_with_token(all_columns, 'Total')
moe_columns = find_columns_with_token(all_columns, 'Margin of Error')

#remove median related columns
median_columns = find_columns_with_token_partial_match(all_columns, 'Median')
median_moe_columns = find_columns_with_token_partial_match(
    median_columns, 'Margin of Error')
# print(json.dumps(median_moe_columns, indent = 2))
print(len(median_columns))
total_columns = list(set(total_columns) - set(median_columns))
total_columns = list(set(total_columns) - set(moe_columns))

denominators_section = {}
# total_replacements = ['Moved; within same county', 'Moved; from different county, same state', 'Moved; from different  state', 'Moved; from abroad']
total_replacements = [
    'Moved; within same municipio', 'Moved; from different municipio',
    'Moved; from the U.S.', 'Moved; from outside Puerto Rico and the U.S.'
]

# denominators_section = replaceTokenListInColumnList(total_columns, 'Total', ['Moved; within same county', 'Moved; from different county, same state', 'Moved; from different  state', 'Moved; from abroad'])
for column_name in total_columns:
  denominators_section[column_name] = []
  for token in total_replacements:
    new_column = replace_token_in_column(column_name, 'Total', token)
    new_column_moe = replace_token_in_column(column_name, 'Total',
                                             token + ' MOE')
    if new_column in all_columns:
      denominators_section[column_name].append(new_column)
    temp_str = replace_token_in_column(new_column, 'Estimate',
                                       'Margin of Error')
    if temp_str in all_columns:
      denominators_section[column_name].append(temp_str)
    temp_str = replace_token_in_column(new_column_moe, 'Estimate',
                                       'Margin of Error')
    if temp_str in all_columns:
      denominators_section[column_name].append(temp_str)
# denominators_section = replaceTokenListInColumnList(total_columns, 'Total', ['Moved; within same municipio', 'Moved; from different municipio', 'Moved; from the U.S.', 'Moved; from outside Puerto Rico and the U.S.'])

print(json.dumps(denominators_section, indent=2))

total_moe_columns = find_columns_with_token(all_columns, 'Total MOE')
median_moe_columns = find_columns_with_token_partial_match(
    total_moe_columns, 'Median')
print(len(median_moe_columns))
print(json.dumps(median_moe_columns, indent=2))
total_moe_columns = list(set(total_moe_columns) - set(median_moe_columns))
# denominators_section.update(replaceTokenListInColumnList(total_moe_columns, 'Total MOE', ['Moved; within same county MOE', 'Moved; from different county, same state MOE', 'Moved; from different  state MOE', 'Moved; from abroad MOE']))
denominators_section.update(
    replace_token_list_in_column_list(total_moe_columns, 'Total MOE', [
        'Moved; within same municipio MOE',
        'Moved; from different municipio MOE', 'Moved; from the U.S. MOE',
        'Moved; from outside Puerto Rico and the U.S. MOE'
    ]))

print(json.dumps(denominators_section, indent=2))

median_moe = find_columns_with_token(median_columns, 'Margin of Error')
median_est = find_columns_with_token(median_columns, 'Estimate')
replaced_columns = replace_token_in_column_list(median_est, 'Estimate',
                                                'Margin of Error')
print('margin', len(find_columns_with_token(median_columns, 'Margin of Error')))
print('estimate', len(find_columns_with_token(median_columns, 'Estimate')))
print(len(find_columns_with_token_partial_match(total_moe_columns, 'Median')))
print(list(set(median_moe) - set(replaced_columns)))

print(list(set(total_columns) - set(median_columns)))
total_columns = list(set(total_columns) - set(median_columns))
replaced_columns = replace_token_in_column_list(total_columns, 'Estimate',
                                                'Margin of Error')
moe_columns = find_columns_with_token(all_columns, 'Margin of Error')

print(json.dumps(total_columns, indent=1))
print(json.dumps(list(set(replaced_columns) - set(moe_columns)), indent=2))

print(len(total_moe_columns))
print(len(find_columns_with_token(median_columns, 'Margin of Error')))
print(len(find_columns_with_token_partial_match(all_columns, 'Median')))
