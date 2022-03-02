from operator import imod
import unittest
from acs_spec_validator import *

class TestCommonUtil(unittest.TestCase):
    def test_find_extra_tokens(self):
        spec1 = {
            'pvs': {
                'p1': {
                    'a': 'v1',
                    'b': 'v2'
                },
                'p2': {
                    'c': 'v3'
                }
            }
        }
        self.assertEqual(find_extra_tokens(['a!!c', 'a'], spec1), ['b'])
        self.assertEqual(find_extra_tokens(['a!!c', 'b!!c'], spec1), [])
        
    def test_find_columns_with_no_properties(self):
        spec1 = {
            'pvs': {
                'p1': {
                    'a': 'v1',
                    'b': 'v2'
                },
                'p2': {
                    'c': 'v3'
                }
            }
        }
        self.assertEqual(find_columns_with_no_properties(['d!!e', 'a!!c'], spec1), ['d!!e'])
        self.assertEqual(find_columns_with_no_properties(['a!!c', 'b!!c'], spec1), [])
    
    def test_find_ignore_conflicts(self):
        spec1 = {
            'pvs': {
                'p1': {
                    'a': 'v1',
                    'b': 'v2'
                },
                'p2': {
                    'c': 'v3'
                }
            },
            'ignoreColumns': ['a']
        }
        spec2 = copy.deepcopy(spec1)
        spec2['ignoreColumns'] = ['d']

        self.assertEqual(find_ignore_conflicts(spec1), ['a'])
        self.assertEqual(find_ignore_conflicts(spec2), [])
    
    # TODO find_missing_enum_specialisation

    def test_find_multiple_measurement(self):
        spec1 = {
            'measurement': {
                    'a': 'v1',
                    'b': 'v2',
                    'c': 'v3'
                }
        }
        
        self.assertEqual(find_multiple_measurement(['a!!d', 'b!!e'], spec1), [])
        self.assertEqual(find_multiple_measurement(['a!!c!!d', 'b!!e'], spec1), ['a!!c!!d'])

    def test_find_multiple_population(self):
        spec1 = {
            'populationType': {
                    'a': 'v1',
                    'b': 'v2',
                    'c': 'v3'
                }
        }
        
        self.assertEqual(find_multiple_population(['a!!d', 'b!!e'], spec1), [])
        self.assertEqual(find_multiple_population(['a!!c!!d', 'b!!e'], spec1), ['a!!c!!d'])
    
    def test_find_missing_denominator_total_column(self):
        spec1 = {
            'denominators': {
                'a!!b!!c': ['a!!b!!c!!d', 'a!!b!!c!!e'],
                'f!!g!!h': ['f!!g!!h!!d', 'a!!b!!c!!e']
            }
        }
        
        self.assertEqual(find_missing_denominator_total_column(['a!!b!!c', 'f!!g!!h'], spec1), [])
        self.assertEqual(find_missing_denominator_total_column(['a!!b!!c', 'f!!g!!h!!d'], spec1), ['f!!g!!h'])
    
    def test_find_missing_denominators(self):
        spec1 = {
            'denominators': {
                'a!!b!!c': ['a!!b!!c!!d', 'a!!b!!c!!e'],
                'f!!g!!h': ['f!!g!!h!!d', 'a!!b!!c!!e']
            }
        }
        
        self.assertEqual(find_missing_denominators(['a!!b!!c', 'f!!g!!h', 'a!!b!!c!!d', 'a!!b!!c!!e', 'f!!g!!h!!d'], spec1), [])
        self.assertEqual(sorted(find_missing_denominators(['a!!b!!c', 'f!!g!!h!!d'], spec1)), sorted(['a!!b!!c!!d', 'a!!b!!c!!e']))
    
    def test_find_repeating_denominators(self):
        spec1 = {
            'denominators': {
                'a!!b!!c': ['a!!b!!c!!d', 'a!!b!!c!!e'],
                'f!!g!!h': ['f!!g!!h!!d', 'a!!b!!c!!e']
            }
        }
        
        spec2 = copy.deepcopy(spec1)
        spec2['denominators']['f!!g!!h'] = ['a!!b', 'c!!d']
        self.assertEqual(find_repeating_denominators(spec2), [])
        self.assertEqual(find_repeating_denominators(spec1), ['a!!b!!c!!e'])
    
    def test_find_extra_inferred_properties(self):
        spec1 = {
            'pvs': {
                'p1': {
                    'a': 'v1',
                    'b': 'v2'
                },
                'p2': {
                    'c': 'v3'
                }
            },
            'inferredSpec': {
                'p1': {
                    'p3': 'v4'
                }
            }
        }
        
        spec2 = copy.deepcopy(spec1)
        spec2['inferredSpec']['p4'] = {'p5': 'v5'}
        self.assertEqual(find_extra_inferred_properties(spec1), [])
        self.assertEqual(find_extra_inferred_properties(spec2), ['p4'])

if __name__ == '__main__':
    unittest.main()