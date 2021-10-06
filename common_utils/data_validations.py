import csv 
import os
import re


csv_path = os.path.expanduser('~/Documents/acs_tables/S0701/run2/S0701_cleaned.csv')
out_csv_path = os.path.expanduser('~/Documents/acs_tables/S0701/run2/S0701_cleaned_fixed.csv')
log_csv_path = os.path.expanduser('~/Documents/acs_tables/S0701/run2/S0701_fix_required.csv')

csv_reader = csv.reader(open(csv_path, 'r'))
csv_writer = csv.writer(open(out_csv_path, 'w'))
log_csv_writer = csv.writer(open(log_csv_path, 'w'))

for row in csv_reader:
	if csv_reader.line_num == 1:
		csv_writer.writerow(row)
		log_csv_writer.writerow(row)
	else:
		try:
			float(row[3])
			csv_writer.writerow(row)
		except ValueError:
			print(','.join(row))
			log_csv_writer.writerow(row)
			row[3] = re.sub("[^0-9.]", "", row[3])
			csv_writer.writerow(row)
			# print(row[3])
