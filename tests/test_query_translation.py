#
# Copyright (c) 2015 Intel Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import json
import unittest
from ddt import ddt, data, unpack

from data_catalog.query_translation import ElasticSearchQueryTranslator, \
    ElasticSearchFilterExtractor, ElasticSearchBaseQueryCreator, InvalidQueryError
from unittest import TestCase


@ddt
class FilterExtractorTests(TestCase):

    def setUp(self):
        self.filter_extractor = ElasticSearchFilterExtractor()

    # first uuids (list), then input filters (list),
    # then output query filters (json)
    # then output post_filters (json)
    # then dataset_filtering value (True, False, None)
    example_singleFilter_org = (
        ['org-id-001'],
        [{'format': ['csv']}],
        {
            'or': [
                {'term': {'orgUUID': 'org-id-001'}},
                {'term': {'isPublic': 'true'}}
            ]
        },
        {
            'and': [
                {'term': {'format': 'csv'}}
            ]
        },
        None
    )

    example_singleFilter_onlyPublic = (
        ['org-id-001'],
        [{'format': ['csv']}],
        {
            'and': [
                {'term': {'isPublic': 'true'}}
            ]
        },
        {
            'and': [
                {'term': {'format': 'csv'}}
            ]
        },
        True
    )

    example_singleFilter_onlyPrivate = (
        ['org-id-001'],
        [{'format': ['csv']}],
        {
            'and': [
                {'term': {'orgUUID': 'org-id-001'}},
                {'term': {'isPublic': 'false'}}
            ]
        },
        {
            'and': [
                {'term': {'format': 'csv'}}
            ]
        },
        False
    )

    example_multivaluedFilterQuery_org = (
        ['org-id-002'],
        [
            {'category': ['health', 'finance']}
        ],
        {
            'or': [
                {'term': {'orgUUID': 'org-id-002'}},
                {'term': {'isPublic': 'true'}}
            ]
        },
        {
            'and': [
                {'terms': {'category': ['health', 'finance']}}
            ]
        },
        None
    )

    example_multivaluedFilterQuery_onlyPublic = (
        ['org-id-002'],
        [
            {'category': ['health', 'finance']}
        ],
        {
            'and': [
                {'term': {'isPublic': 'true'}}
            ]
        },
        {
            'and': [
                {'terms': {'category': ['health', 'finance']}}
            ]
        },
        True
    )

    example_multivaluedFilterQuery_onlyPrivate = (
        ['org-id-002'],
        [
            {'category': ['health', 'finance']}
        ],
        {
            'and': [
                {'term': {'orgUUID': 'org-id-002'}},
                {'term': {'isPublic': 'false'}}
            ]
        },
        {
            'and': [
                {'terms': {'category': ['health', 'finance']}}
            ]
        },
        False
    )

    example_multipleFilterQuery_org = (
        ['org-id-003'],
        [
            {'format': ['csv']},
            {'category': ['health']}
        ],
        {
            'or': [
                {'term': {'orgUUID': 'org-id-003'}},
                {'term': {'isPublic': 'true'}}
            ]
        },
        {
            'and': [
                {'term': {'format': 'csv'}},
                {'term': {'category': 'health'}}
            ]
        },
        None
    )

    example_multipleFilterQuery_onlyPublic = (
        ['org-id-003'],
        [
            {'format': ['csv']},
            {'category': ['health']}
        ],
        {
            'and': [
                {'term': {'isPublic': 'true'}}
            ]
        },
        {
            'and': [
                {'term': {'format': 'csv'}},
                {'term': {'category': 'health'}}
            ]
        },
        True
    )

    example_multipleFilterQuery_onlyPrivate = (
        ['org-id-003'],
        [
            {'format': ['csv']},
            {'category': ['health']}
        ],
        {
            'and': [
                {'term': {'orgUUID': 'org-id-003'}},
                {'term': {'isPublic': 'false'}}
            ]
        },
        {
            'and': [
                {'term': {'format': 'csv'}},
                {'term': {'category': 'health'}}
            ]
        },
        False
    )

    example_upperCaseFilterValue_org = (
        ['org-id-004'],
        [
            {'format': ['CSV']}
        ],
        {
            'or': [
                {'term': {'orgUUID': 'org-id-004'}},
                {'term': {'isPublic': 'true'}}
            ]
        },
        {
            'and': [
                {'term': {'format': 'csv'}}
            ]
        },
        None
    )

    example_upperCaseFilterValue_onlyPublic = (
        ['org-id-004', 'public'],
        [
            {'format': ['CSV']}
        ],
        {
            'and': [
                {'term': {'isPublic': 'true'}}
            ]
        },
        {
            'and': [
                {'term': {'format': 'csv'}}
            ]
        },
        True
    )

    example_upperCaseFilterValue_onlyPrivate = (
        ['org-id-004'],
        [
            {'format': ['CSV']}
        ],
        {
            'and': [
                {'term': {'orgUUID': 'org-id-004'}},
                {'term': {'isPublic': 'false'}}
            ]
        },
        {
            'and': [
                {'term': {'format': 'csv'}}
            ]
        },
        False
    )

    example_fromToTimeQuery_org = (
        ['org-id-005'],
        [
            {'creationTime': ['2014-05-18', '2014-11-03']}
        ],
        {
            'and': [
                {'range': {'creationTime': {'from': '2014-05-18', 'to': '2014-11-03'}}},
                {
                    'or': [
                        {'term': {'orgUUID': 'org-id-005'}},
                        {'term': {'isPublic': 'true'}}
                    ]
                }

            ]
        },
        {},
        None
    )

    example_fromToTimeQuery_onlyPublic = (
        ['org-id-005'],
        [
            {'creationTime': ['2014-05-18', '2014-11-03']}
        ],
        {
            'and': [
                {'range': {'creationTime': {'from': '2014-05-18', 'to': '2014-11-03'}}},
                {'term': {'isPublic': 'true'}}
            ]
        },
        {},
        True
    )

    example_fromToTimeQuery_onlyPrivate = (
        ['org-id-005'],
        [
            {'creationTime': ['2014-05-18', '2014-11-03']}
        ],
        {
            'and': [
                {'range': {'creationTime': {'from': '2014-05-18', 'to': '2014-11-03'}}},
                {'term': {'orgUUID': 'org-id-005'}},
                {'term': {'isPublic': 'false'}}
            ]
        },
        {},
        False
    )

    example_beforeTimeQuery_org = (
        ['org-id-006'],
        [
            {'creationTime': [-1, '2014-11-03']}
        ],
        {
            'and': [
                {'range': {'creationTime': {'to': '2014-11-03'}}},
                {
                    'or': [
                        {'term': {'orgUUID': 'org-id-006'}},
                        {'term': {'isPublic': 'true'}}
                    ]
                }

            ]
        },
        {},
        None
    )

    example_afterTimeQuery_org = (
        ['org-id-007'],
        [
            {'creationTime': ['2014-05-18', -1]}
        ],
        {
            'and': [
                {'range': {'creationTime': {'from': '2014-05-18'}}},
                {
                    'or': [
                        {'term': {'orgUUID': 'org-id-007'}},
                        {'term': {'isPublic': 'true'}}
                    ]
                }

            ]
        },
        {},
        None
    )

    @data(example_singleFilter_org,
          example_singleFilter_onlyPublic,
          example_singleFilter_onlyPrivate,
          example_multivaluedFilterQuery_org,
          example_multivaluedFilterQuery_onlyPublic,
          example_multivaluedFilterQuery_onlyPrivate,
          example_multipleFilterQuery_org,
          example_multipleFilterQuery_onlyPublic,
          example_multipleFilterQuery_onlyPrivate,
          example_upperCaseFilterValue_org,
          example_upperCaseFilterValue_onlyPublic,
          example_upperCaseFilterValue_onlyPrivate,
          example_fromToTimeQuery_org,
          example_fromToTimeQuery_onlyPublic,
          example_fromToTimeQuery_onlyPrivate,
          example_beforeTimeQuery_org,
          example_afterTimeQuery_org
          )
    @unpack
    def test_filterExtraction_properFilter_filterExtracted(self,
                                                           org_uuid_list,
                                                           input_filters,
                                                           query_filters,
                                                           post_filters,
                                                           dataset_filtering):
        self._assert_filter_extraction_ddt(org_uuid_list,
                                           input_filters,
                                           query_filters,
                                           post_filters,
                                           dataset_filtering)

    example_nonListAsFilterValues = (
        {'filters': [{'filter name': 'filter value'}]},
        ['org-id-008'],
        True
    )

    example_nonDictAsFilter = (
        {'filters': ['not a dictionary']},
        ['org-id-09'],
        True
    )

    example_invalidFilterName = (
        {'filters': [{'nonexistent_mapping_field': ['some value']}]},
        ['org-id-010'],
        True
    )

    example_wrongNumberTimeParameters = (
        {'filters': [{'creationTime': ['2014-11-03', '2014-11-04', '2014-11-05']}]},
        ['org-id-011'],
        True
    )

    # @data(example_nonListAsFilterValues,
    #       example_nonDictAsFilter,
    #       example_invalidFilterName,
    #       example_wrongNumberTimeParameters)
    # @unpack
    # def test_filterExtractionErrors_improperFilter_invalidQueryError(self,
    #                                                                  invalid_filters,
    #                                                                  org_uuid_list,
    #                                                                  dataset_filtering):
    #     with self.assertRaises(InvalidQueryError):
    #         self.filter_extractor.extract_filter(invalid_filters, org_uuid_list, dataset_filtering)

    def _assert_filter_extraction_ddt(self,
                                      org_uuid_list,
                                      input_filters,
                                      test_query_filter,
                                      test_post_filter,
                                      dataset_filtering):
        """input_filters -- Dictionary of list of dictionaries in a form:
        {'filters': [
            {filter_name: [filter_value_1, ...]},
            {filter_name2: [filter_value_2_1, ...]}
        ]}"""
        filters = {'filters': input_filters}
        output_filter, post_filter = self.filter_extractor.extract_filter(filters, org_uuid_list, dataset_filtering, False)
        self.assertDictEqual(test_query_filter, output_filter)
        self.assertDictEqual(test_post_filter, post_filter)


class ElasticSearchBaseQueryCreationTests(TestCase):
    MATCH_ALL = {'match_all': {}}

    def setUp(self):
        self.query_creator = ElasticSearchBaseQueryCreator()

    def test_baseQueryCreation_textQueryProvided_baseQueryCreated(self):
        TEXT = 'some text query'
        proper_base_query = {
            'bool': {
                'should': [
                    {
                        'match': {
                            'title': {
                                'query': TEXT,
                                'boost': 3,
                                'fuzziness': 1
                            }
                        }
                    },
                    {
                        'match': {
                            'dataSample': {
                                'query': TEXT,
                                'boost': 2
                            }
                        }
                    },
                    {
                        'match': {
                            'sourceUri': {
                                'query': TEXT,
                            }
                        }
                    }
                ]
            }
        }

        self.assertDictEqual(
            proper_base_query,
            self.query_creator.create_base_query({'query': TEXT}))

    def test_baseQueryCreation_noQueryElement_matchAllReturned(self):
        self.assertDictEqual(
            self.MATCH_ALL,
            self.query_creator.create_base_query({}))

    def test_baseQueryCreation_emptyQuery_matchAllReturned(self):
        self.assertDictEqual(
            self.MATCH_ALL,
            self.query_creator.create_base_query({'query': ''}))


class ElasticSearchQueryTranslationTests(TestCase):
    def setUp(self):
        self.translator = ElasticSearchQueryTranslator()
        self.org_uuid = ['orgid007']

    def test_queryTranslation_sizeInQuery_sizeAddedToOutput(self):
        SIZE = 123
        size_query = json.dumps({'size': SIZE})

        translated_query = self.translator.translate(size_query, self.org_uuid, None, False)

        self.assertEqual(SIZE, json.loads(translated_query)['size'])

    def test_queryTranslation_fromInQuery_fromAddedToOutput(self):
        FROM = 345
        from_query = json.dumps({'from': FROM})

        translated_query = self.translator.translate(from_query, self.org_uuid, True, False)

        self.assertEqual(FROM, json.loads(translated_query)['from'])

    def test_combiningQueryAndFilter_queryWithFilter_filteredQueryCreated(self):
        FAKE_BASE_QUERY = {'yup': 'totally fake'}
        FAKE_FILTER = {'uhuh': 'this filter is also fake'}
        FAKE_POST_FILTER = {'hello': 'fake filter'}
        expected_query = {
            'query': {
                'filtered': {
                    'filter': FAKE_FILTER,
                    'query': FAKE_BASE_QUERY
                }
            },
            'post_filter': FAKE_POST_FILTER,
            'aggregations': {
                'categories': {
                    'terms': {
                        'field': 'category'
                    }
                },
                'formats': {
                    'terms': {
                        'field': 'format'
                    }
                }
            }
        }

        output_query = self.translator._combine_query_and_filters(FAKE_BASE_QUERY, FAKE_FILTER, FAKE_POST_FILTER)

        self.assertDictEqual(expected_query, output_query)

    def test_queryTranslation_queryIsNotJson_invalidQueryError(self):
        with self.assertRaises(InvalidQueryError):
            self.translator.translate('{"this is not a proper JSON"}', self.org_uuid, None, False)

    def test_decodingInputQuery_noneQuery_emptyDictReturned(self):
        self.assertDictEqual(
            {},
            self.translator._get_query_dict(None))

    def test_queryTranslation_fullFeaturedQuery_queryTranslated(self):
        input_query = {
            'query': 'blabla',
            'filters': [
                {'format': ['csv']}
            ],
            'size': 3,
            'from': 14
        }

        output_query_string = self.translator.translate(json.dumps(input_query), self.org_uuid, True, False)
        output_query = json.loads(output_query_string)

        self.assertIn('filtered', output_query['query'])
        self.assertIn('size', output_query)
        self.assertIn('from', output_query)


if __name__ == '__main__':
    unittest.main()
