import copy
import ast
import json
import os
import sys
from absl import app
from absl import flags

FLAGS = flags.FLAGS

module_dir_ = os.path.dirname(__file__)
sys.path.append(os.path.join(module_dir_, '..'))

flags.DEFINE_string('dcid', None, 'Path to zip file downloaded from US Census')
flags.DEFINE_string('dc_output_path', './prefetched_outputs/',
                    'Path to zip file downloaded from US Census')
flags.DEFINE_boolean('force_fetch', False,
                     'Produce a list of tokens from the input file/s')

from common_utils.common_util import request_url_json

# fetch pvs from dc, enums from dc


def fetch_dcid_properties_enums(class_dcid,
                                output_path=module_dir_ + '/prefetched_outputs',
                                force_fetch=False):
    output_path = os.path.expanduser(output_path)
    if not os.path.exists(output_path):
        os.makedirs(output_path, exist_ok=True)

    dc_props = {}

    # get list of properties for each population type
    if force_fetch or not os.path.isfile(
            os.path.join(output_path, f'{class_dcid}_dc_props.json')):
        population_props = request_url_json(
            f'https://autopush.api.datacommons.org/node/property-values?dcids={class_dcid}&property=domainIncludes&direction=in'
        )
        dc_population_pvs = population_props['payload']
        dc_population_pvs = ast.literal_eval(dc_population_pvs)

        if dc_population_pvs[class_dcid]:
            dc_props = {}
            for prop_dict in dc_population_pvs[class_dcid]['in']:
                dc_props[prop_dict['dcid']] = []

        with open(os.path.join(output_path, f'{class_dcid}_dc_props.json'),
                  'w') as fp:
            json.dump(dc_props, fp, indent=2)
    else:
        dc_props = json.load(
            open(os.path.join(output_path, f'{class_dcid}_dc_props.json'), 'r'))

    # check if the list has enum type
    if force_fetch or not os.path.isfile(
            os.path.join(output_path, f'{class_dcid}_dc_props_types.json')):
        population_props_types = request_url_json(
            f'https://autopush.api.datacommons.org/node/property-values?dcids={"&dcids=".join(dc_props.keys())}&property=rangeIncludes&direction=out'
        )
        population_props_types = ast.literal_eval(
            population_props_types['payload'])
        for property_name in population_props_types:
            for temp_dict in population_props_types[property_name]['out']:
                dc_props[property_name].append(temp_dict['dcid'])

        # print(population_props_types)

        with open(
                os.path.join(output_path, f'{class_dcid}_dc_props_types.json'),
                'w') as fp:
            json.dump(dc_props, fp, indent=2)

        # print(population_props_enums)
    else:
        dc_props = json.load(
            open(os.path.join(output_path, f'{class_dcid}_dc_props_types.json'),
                 'r'))

    # get enum value list
    if force_fetch or not os.path.isfile(
            os.path.join(output_path,
                         f'{class_dcid}_dc_props_enum_values.json')):
        new_dict = copy.deepcopy(dc_props)
        for property_name in new_dict.keys():
            dc_props[property_name] = []
            for type_name in new_dict[property_name]:
                if 'enum' in type_name.lower():
                    enum_values = request_url_json(
                        f'https://autopush.api.datacommons.org/node/property-values?dcids={type_name}&property=typeOf&direction=in'
                    )
                    enum_values = ast.literal_eval(enum_values['payload'])
                    if enum_values[type_name]:
                        for temp_dict in enum_values[type_name]['in']:
                            dc_props[property_name].append(temp_dict['dcid'])

        with open(
                os.path.join(output_path,
                             f'{class_dcid}_dc_props_enum_values.json'),
                'w') as fp:
            json.dump(dc_props, fp, indent=2)
    else:
        dc_props = json.load(
            open(
                os.path.join(output_path,
                             f'{class_dcid}_dc_props_enum_values.json'), 'r'))

    return dc_props


def main(argv):
    print(
        json.dumps(fetch_dcid_properties_enums(FLAGS.dcid, FLAGS.dc_output_path,
                                               FLAGS.force_fetch),
                   indent=2))


if __name__ == '__main__':
    flags.mark_flags_as_required(['dcid'])
    app.run(main)
