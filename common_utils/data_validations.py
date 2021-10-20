import csv 
import os
import re
from absl import flags, app

FLAGS = flags.FLAGS
flags.DEFINE_string('csv_path', None, 'Path to the input csv')
flags.DEFINE_string('column_tag', 'Quantity', 'Column name of the column with values to be checked')

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
			if FLAGS.column_tag in row:
				column_index = row.index(FLAGS.column_tag)
		else:
			try:
				float(row[column_index])
				csv_writer.writerow(row)
			except ValueError:
				print(','.join(row))
				log_csv_writer.writerow(row)
				row[column_index] = re.sub("[^0-9.]", "", row[column_index])
				csv_writer.writerow(row)
				# print(row[3])

if __name__ == '__main__':
	flags.mark_flags_as_required(['csv_path'])
	app.run(fix_open_distributions_in_observation_csv)
