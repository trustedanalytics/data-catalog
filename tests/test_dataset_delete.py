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

import requests
from mock import MagicMock, call
from ddt import ddt, data, unpack
from elasticsearch.exceptions import NotFoundError, ConnectionError
from data_catalog.dataset_delete import (DataSetRemover, DataSourceServiceError,
                                         NotFoundInExternalService)
from tests.base_test import DataCatalogTestCase


@ddt
class DataSetDeleteTest(DataCatalogTestCase):
    DATA_SET_ID = 'test-entry-id'
    AUTH_TOKEN = 'authorization-token'
    DATABASE_ID = 'database_id'
    TARGET_URI = 'hdfs://URI/DATA/{}/000000_1'.format(DATABASE_ID)
    MOCK_GET = {'_source': {'targetUri': TARGET_URI}}

    def setUp(self):
        super(DataSetDeleteTest, self).setUp()

        self._delete_obj = DataSetRemover()
        self._delete_obj._elastic_search.delete = self._mock_es_delete = MagicMock()
        self._delete_obj._elastic_search.get = self._mock_es_get = MagicMock()
        self._delete_obj._elastic_search.indices.flush = self._mock_es_flush = MagicMock()
        self._mock_es_get.return_value = self.MOCK_GET
        requests.delete = self._mock_req_delete = MagicMock()

    @data(NotFoundError,
          ConnectionError)
    def test_delete_elasticDeleteErroneous(self, error):
        self._assert_elastic_delete(error)

    def _assert_elastic_delete(self, error):
        self._mock_es_delete.side_effect = error
        with self.assertRaises(error):
            self._delete_obj.delete(self.DATA_SET_ID, self.AUTH_TOKEN)

    @data((404, NotFoundInExternalService),
          (500, DataSourceServiceError),
          (999, DataSourceServiceError))
    @unpack
    def test_delete_requestDeleteErroneous_ErrorRaised(self, return_value, error):
        self._assert_request_delete(return_value, error)

    def _assert_request_delete(self, return_value, error):
        self._mock_req_delete.return_value.status_code = return_value

        with self.assertRaises(error):
            self._delete_obj.delete(self.DATA_SET_ID, self.AUTH_TOKEN)
        self._delete_obj._elastic_search.indices.flush.assert_called_with()

    def test_delete_dataSetExists_dataSetDeleted(self):
        self._mock_req_delete.return_value.status_code = 200

        delete_result = self._delete_obj.delete(self.DATA_SET_ID, self.AUTH_TOKEN)

        self.assertEqual(delete_result, None)
        calls = [
            call(self._config.services_url.downloader_url_pattern.format(self.DATABASE_ID),
                 headers={'Authorization': self.AUTH_TOKEN}),
            call(self._config.services_url.dataset_publisher_url,
                 json=self.MOCK_GET["_source"], headers={'Authorization': self.AUTH_TOKEN})
        ]
        self._mock_req_delete.assert_has_calls(calls)

    def test_delete_dataSetExists_withVariantUri_dataSetDeleted(self):
        self._mock_req_delete.return_value.status_code = 200
        self._mock_es_get.return_value = {'_source': {'targetUri': self.TARGET_URI}}

        delete_result = self._delete_obj.delete(self.DATA_SET_ID, self.AUTH_TOKEN)

        self.assertEqual(delete_result, None)
        calls = [
            call(self._config.services_url.downloader_url_pattern.format(self.DATABASE_ID),
                 headers={'Authorization': self.AUTH_TOKEN}),
            call(self._config.services_url.dataset_publisher_url,
                 json=self.MOCK_GET["_source"], headers={'Authorization': self.AUTH_TOKEN})
        ]
        self._mock_req_delete.assert_has_calls(calls)
