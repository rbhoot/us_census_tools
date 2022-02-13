# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Utilities to fix or detect unexpected observation quantity values.
"""
import csv
import os
import re
from absl import flags, app

FLAGS = flags.FLAGS
flags.DEFINE_string('csv_path', None, 'Path to the input csv')
flags.DEFINE_string('quantity_tag', 'Quantity',
                    'Column name of the column with values to be checked')
flags.DEFINE_string('geo_tag', 'Place',
                    'Column name of the column with geo ids')
flags.DEFINE_string('statvar_tag', 'StatVar',
                    'Column name of the column with StatVar id')
flags.DEFINE_multi_enum('data_tests', ['all'],
                        ['all', 'open_distributions', 'possible_percentage'],
                        'List of tests to run')


def fix_open_distributions_in_observation_csv(csv_path: str, quantity_tag: str = 'Quantity') -> None:
  """
        Function to fix the open distributions in the StatVarObservation values
        The function creates 2 different output files:
        (Note: .csv is expected at end of filename)
          - file name suffixed with `_fixed`. 
            This file contains the quantities with non numeric digits dropped.
          - file name suffixed with `_fix_log`
            This file has record of orginal values for future reference.
        
        Args:
          csv_path: String pointing to the path of the source csv file.
          quantity_tag: Header name of the column containing the Quantity values to be screened.
        """

  csv_path = os.path.abspath(os.path.expanduser(csv_path))
  out_csv_path = csv_path.replace('.csv', '_fixed.csv')
  log_csv_path = csv_path.replace('.csv', '_fix_log.csv')

  csv_reader = csv.reader(open(csv_path, 'r'))
  csv_writer = csv.writer(open(out_csv_path, 'w'))
  log_csv_writer = csv.writer(open(log_csv_path, 'w'))

  column_index = 3
  for row in csv_reader:
    if csv_reader.line_num == 1:
      csv_writer.writerow(row)
      log_csv_writer.writerow(row)
      # Find the index of column with quantity values
      if quantity_tag in row:
        column_index = row.index(quantity_tag)
    else:
      # if not a float, drop non digit charachters
      try:
        float(row[column_index])
        csv_writer.writerow(row)
      except ValueError:
        print(','.join(row))
        # log the original row in the log file.
        log_csv_writer.writerow(row)
        row[column_index] = re.sub('[^0-9.]', '', row[column_index])
        csv_writer.writerow(row)

def detect_percentages_in_observation_csv(csv_path: str, quantity_tag: str = 'Quantity', geo_tag: str = 'Place', statvar_tag: str = 'StatVar') -> None:
  """
        Function to detect possible percentage values in the StatVarObservation values.
        Function screens the csv by checking if any StatVar has an observation value below 100
          with country/USA as the place of observation.
        The function creates a new file with suffix `_possible_percentages`.
        (Note: .csv is expected at end of filename)
        
        Args:
          csv_path: String pointing to the path of the source csv file.
          quantity_tag: Header name of the column containing the Quantity values to be screened.
          geo_tag: Header name of the column containing the observation place.
          statvar_tag: Header name of the column containing the observed stat var.
        """

  csv_path = os.path.abspath(os.path.expanduser(csv_path))
  log_csv_path = csv_path.replace('.csv', '_possible_percentages.csv')

  csv_reader = csv.reader(open(csv_path, 'r'))
  log_csv_writer = csv.writer(open(log_csv_path, 'w'))

  # Assumed index
  column_index = 3
  geo_index = 1
  statvar_index = 2

  # List of stat vars that could have less than 100 values and hence could be ignored from screening.
  ignore_stat_str = ['Mean', 'Median', 'MarginOfError']

  for row in csv_reader:
    if csv_reader.line_num == 1:
      log_csv_writer.writerow(row)
      # Find the index of each column type
      if quantity_tag in row:
        column_index = row.index(quantity_tag)
      if geo_tag in row:
        geo_index = row.index(geo_tag)
      if statvar_tag in row:
        statvar_index = row.index(statvar_tag)
    else:
      try:
        val = float(row[column_index])
        if row[geo_index] == 'country/USA':
          if val <= 100:
            to_ignore = False
            # Ignore special stat vars like mean, median and margin of errors
            for token in ignore_stat_str:
              if token in row[statvar_index]:
                to_ignore = True
            if not to_ignore:
              log_csv_writer.writerow(row)
              print('Warning: Found a row with percentage value')
      except ValueError:
        print(
            'Warning: Found open distributions, run with --data_tests=open_distributions and use the fixed csv file'
        )


def main(argv):
  if 'all' in FLAGS.data_tests or 'open_distributions' in FLAGS.data_tests:
    fix_open_distributions_in_observation_csv(FLAGS.csv_path, FLAGS.quantity_tag)
  if 'all' in FLAGS.data_tests or 'possible_percentage' in FLAGS.data_tests:
    if 'all' in FLAGS.data_tests:
      detect_percentages_in_observation_csv(FLAGS.csv_path.replace('.csv', '_fixed.csv'), FLAGS.quantity_tag, FLAGS.geo_tag, FLAGS.statvar_tag)
    else:  
      detect_percentages_in_observation_csv(FLAGS.csv_path, FLAGS.quantity_tag, FLAGS.geo_tag, FLAGS.statvar_tag)


if __name__ == '__main__':
  flags.mark_flags_as_required(['csv_path'])
  app.run(main)
