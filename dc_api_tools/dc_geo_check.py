
import csv
import json
import os
import sys
import ast
from absl import app
from absl import flags

FLAGS = flags.FLAGS

module_dir_ = os.path.dirname(__file__)
sys.path.append(os.path.join(module_dir_, '..'))

from common_utils.common_util import request_url_json, requests_post_json

flags.DEFINE_string('column_name', 'Place', 'column name of the column containing geoIds in csv')
flags.DEFINE_boolean('force_fetch', False,
                     'forces api query and not return cached result')

# column_name = 'Place'
# force_fetch = True
# csv_path = '~/dc_clones/dcid_gen/data/scripts/fbi/hate_crime/total_incidents.csv'


def check_geoId_csv(csv_path, column_name, force_fetch):
  csv_path = os.path.expanduser(csv_path)
  cache_geo = {}
  cur_cache_geo = {}
  # read cache
  if os.path.isfile('results_cache.json'):
    with open('results_cache.json') as fp:
      cache_geo = json.load(fp)
  # open csv
  geo_list_req = []
  geo_list = []
  with open(csv_path) as fp:
    csv_reader = csv.DictReader(fp)
    for row in csv_reader:
      # read location
      cur_geo = row[column_name]
      if cur_geo not in geo_list:
        geo_list.append(cur_geo)
      # request api
      if cur_geo not in cache_geo or force_fetch:
        if cur_geo not in geo_list_req:
          geo_list_req.append(cur_geo)


  if geo_list_req:
    data_ = {}
    chunk_size = 400
    geo_list_chunked = [geo_list_req[i:i + chunk_size] for i in range(0, len(geo_list_req), chunk_size)]
    for geo_chunk in geo_list_chunked:
      data_["dcids"] = geo_chunk
      req = requests_post_json('https://api.datacommons.org/node/property-labels', data_)
      geo_dicts = req['payload']
      geo_dicts = ast.literal_eval(geo_dicts)
      for cur_geo in geo_dicts:
        if not geo_dicts[cur_geo]['inLabels'] and not geo_dicts[cur_geo]['outLabels']:
          cache_geo[cur_geo] = False
        else:
          cache_geo[cur_geo] = True
  
  for cur_geo in geo_list:
    if not cache_geo[cur_geo]:
      print(cur_geo)

  # write cache
  with open('results_cache.json', 'w') as fp:
    json.dump(cache_geo, fp, indent=2)

  print('End of script')

def main(argv):
  check_geoId_csv(FLAGS.csv_path, FLAGS.column_name, FLAGS.force_fetch)

if __name__ == '__main__':
  flags.mark_flags_as_required(['csv_path'])
  app.run(main)