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


def fix_open_distributions_in_observation_csv(argv):
  """
        Function to fix the open distributions in the StatVarObservation values
        """

  csv_path = os.path.abspath(os.path.expanduser(FLAGS.csv_path))
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
      if FLAGS.quantity_tag in row:
        column_index = row.index(FLAGS.quantity_tag)
    else:
      try:
        float(row[column_index])
        csv_writer.writerow(row)
      except ValueError:
        print(','.join(row))
        log_csv_writer.writerow(row)
        row[column_index] = re.sub('[^0-9.]', '', row[column_index])
        csv_writer.writerow(row)
        # print(row[3])


def detect_percentages_in_observation_csv(argv):
  """
        Function to detect percentage values in the StatVarObservation values
        """

  csv_path = os.path.abspath(os.path.expanduser(FLAGS.csv_path))
  log_csv_path = csv_path.replace('.csv', '_possible_percentages.csv')

  csv_reader = csv.reader(open(csv_path, 'r'))
  log_csv_writer = csv.writer(open(log_csv_path, 'w'))

  column_index = 3
  geo_index = 1
  statvar_index = 2

  ignore_stat_str = ['Mean', 'Median', 'MarginOfError']

  for row in csv_reader:
    if csv_reader.line_num == 1:
      log_csv_writer.writerow(row)
      if FLAGS.quantity_tag in row:
        column_index = row.index(FLAGS.quantity_tag)
      if FLAGS.geo_tag in row:
        geo_index = row.index(FLAGS.geo_tag)
      if FLAGS.statvar_tag in row:
        statvar_index = row.index(FLAGS.statvar_tag)
    else:
      try:
        val = float(row[column_index])
        if row[geo_index] == 'country/USA':
          if val <= 100:
            to_ignore = False
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
        # print(row[3])


def main(argv):
  if 'all' in FLAGS.data_tests or 'open_distributions' in FLAGS.data_tests:
    fix_open_distributions_in_observation_csv(argv)
  if 'all' in FLAGS.data_tests or 'possible_percentage' in FLAGS.data_tests:
    detect_percentages_in_observation_csv(argv)


if __name__ == '__main__':
  flags.mark_flags_as_required(['csv_path'])
  app.run(main)
