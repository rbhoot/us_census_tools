import csv
import os
from absl import flags, app

FLAGS = flags.FLAGS
flags.DEFINE_string('csv_path', None, 'Path to the input csv')

def remove_method(argv):
  csv_path = os.path.abspath(os.path.expanduser(FLAGS.csv_path))
  out_csv_path = csv_path.replace('.csv', '_unit.csv')

  csv_reader = csv.reader(open(csv_path, 'r'))
  csv_writer = csv.writer(open(out_csv_path, 'w'))
  
  for row in csv_reader:
    if csv_reader.line_num == 1:
      csv_writer.writerow(row)
    else:
      if row[4] == 'USDollar':
        row[4] = 'InflationAdjustedUSD_CurrentYear'
        csv_writer.writerow(row)
      

if __name__ == '__main__':
  flags.mark_flags_as_required(['csv_path'])
  app.run(remove_method)