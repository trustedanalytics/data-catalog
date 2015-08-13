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
import flask

from elasticsearch.exceptions import RequestError, ConnectionError
from mock import patch, MagicMock

from data_catalog.search import DataSetSearch, InvalidQueryError, IndexConnectionError
from tests.base_test import DataCatalogTestCase


class SearchTests(DataCatalogTestCase):

    def setUp(self):
        super(SearchTests, self).setUp()
        self.fake_org_id = 'orgId001'
        self.test_search_result = {'whateverField': 'something'}
        self.test_total_hits = 666
        self.test_id = 13
        self.test_es_search_results = {
            'hits': {
                'hits': [
                    {
                        '_source': self.test_search_result,
                        '_id': self.test_id
                    }
                ],
                'total': self.test_total_hits
            },
            'aggregations': {
                'categories': {
                    'buckets': [
                        {
                            'key': 'health'
                        },
                        {
                            'key': 'science'
                        }
                    ]
                },
                'formats': {
                    'buckets': [
                        {
                            'key': 'csv'
                        }
                    ]
                }
            }
        }

        self._search_obj = DataSetSearch()
        self._search_obj._translator.translate = self._mock_translate = MagicMock()
        self._search_obj._elastic_search.search = self._mock_es_search = MagicMock()
        self.request_context = self.app.test_request_context('/rest/datasets')
        self.request_context.push()

    def tearDown(self):
        super(SearchTests, self).tearDown()
        self.request_context.pop()

    def test_search_withQuery_queryPassedToIndex(self):
        QUERY_STRING = 'fake data catalog query'
        TRANSLATED_QUERY = 'fake translated query'
        self._mock_es_search.return_value = dict(self.test_es_search_results)
        self._mock_translate.return_value = str(TRANSLATED_QUERY)

        response = self._search_obj.search(QUERY_STRING, self.fake_org_id, True, False)

        self.assertListEqual([self.test_search_result], response['hits'])
        self.assertEqual(self.test_total_hits, response['total'])
        self.assertEqual(self.test_id, response['hits'][0]['id'])
        self._mock_translate.assert_called_once_with(QUERY_STRING, self.fake_org_id, True, False)
        self._mock_es_search.assert_called_once_with(
            index=self._config.elastic.elastic_index,
            doc_type=self._config.elastic.elastic_metadata_type,
            body=TRANSLATED_QUERY)

    def test_search_invalidQuery_invalidQueryErrorRaised(self):
        self._mock_es_search.side_effect = RequestError
        with self.assertRaises(InvalidQueryError):
            self._search_obj.search('an invalid query string', self.fake_org_id, False, False)

    def test_search_noIndexConnection_connectionErrorRaised(self):
        self._mock_es_search.side_effect = ConnectionError
        with self.assertRaises(IndexConnectionError):
            self._search_obj.search('some query string', self.fake_org_id, False, False)

    @patch.object(DataSetSearch, 'search')
    def test_restSearch_withQuery_queryPassedToSearch(self, mock_search):
        flask.g.org_uuid_list = '[orgid001]'
        flask.g.is_admin = False
        test_query = 'fake data catalog query'
        mock_search.return_value = dict(self.test_es_search_results)

        response = self.client.get('{}?query={}&orgs=orgid001'.format('/rest/datasets', test_query))
        org_uuid_list = '[orgid001]'
        self.assertEqual(200, response.status_code)
        self.assertDictEqual(self.test_es_search_results, json.loads(response.data))
        mock_search.assert_called_once_with(test_query, org_uuid_list, None, False)

    @patch.object(DataSetSearch, 'search')
    def test_restSearch_invalidQuery_400Returned(self, mock_search):
        flask.g.is_admin = False
        mock_search.side_effect = InvalidQueryError
        response = self.client.get(self._config.app_base_path + '?query=some_invalid_query')
        self.assertEqual(400, response.status_code)

    @patch.object(DataSetSearch, 'search')
    def test_restSearch_noIndexConnection_500Returned(self, mock_search):
        flask.g.is_admin = False
        mock_search.side_effect = IndexConnectionError
        response = self.client.get(self._config.app_base_path + '?query=some_query')
        self.assertEqual(500, response.status_code)


if __name__ == '__main__':
    unittest.main()
