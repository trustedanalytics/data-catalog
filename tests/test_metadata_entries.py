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
from ddt import ddt, data, unpack
from mock import patch

from data_catalog.dataset_delete import DataSetRemover
from data_catalog.metadata_entry import (MetadataIndexingTransformer, Elasticsearch,
                                         InvalidEntryError, NotFoundError, ConnectionError,
                                         CFNotifier, MetadataEntryResource)
from tests.base_test import DataCatalogTestCase


@ddt
class MetadataEntryTests(DataCatalogTestCase):
    TEST_DATA_SET_ID = 'whatever-id'
    CREATION_TIME_FIELD = 'creationTime'
    CATEGORY_FIELD = 'category'
    ORG_UUID_FIELD = 'orgUUID'
    AUTH_TOKEN = 'authorization-token'
    IS_PUBLIC_FIELD = 'isPublic'

    def setUp(self):
        super(MetadataEntryTests, self).setUp()
        # TODO: refactor of tests so they do not need a full metadata entry
        self.test_entry = {
            '_source': {
                self.ORG_UUID_FIELD: "org02",
                self.CATEGORY_FIELD: 'health',
                'dataSample': 'some sample',
                'format': 'csv',
                'recordCount': 13,
                'size': 99999,
                'sourceUri': 'some uri',
                'targetUri': 'hdfs://6.6.6.6:8200/borker/long-long-hash/9213-154b-a0b9/00000_1',
                'title': 'a great title',
                'isPublic': True,
                self.CREATION_TIME_FIELD: '2015-02-13T13:00:00'
            }
        }

        self.test_entry_index = {
            '_source': {
                self.ORG_UUID_FIELD: "org02",
                self.CATEGORY_FIELD: 'health',
                'dataSample': 'some sample',
                'format': 'csv',
                'recordCount': 13,
                'size': 99999,
                'sourceUri': 'some uri',
                'targetUri': 'hdfs://6.6.6.6:8200/borker/long-long-hash/9213-154b-a0b9/00000_1',
                'title': 'a great title',
                'isPublic': True,
                self.CREATION_TIME_FIELD: '2015-02-13T13:00:00'
            }
        }

        self.TEST_ENTRY_URL = '{0}/{1}'.format('/rest/datasets', self.TEST_DATA_SET_ID)
        self.TEST_BODY = {self.IS_PUBLIC_FIELD: self.test_entry_index['_source'][self.IS_PUBLIC_FIELD]}
        self.get_args = {
            'index': self._config.elastic.elastic_index,
            'doc_type': self._config.elastic.elastic_metadata_type,
            'id': self.TEST_DATA_SET_ID,
        }
        self.index_args = dict(self.get_args)
        self.index_args['body'] = self.test_entry_index['_source']
        self.request_context = self.app.test_request_context('fake_path')
        self.request_context.push()
        flask.g.is_admin = True

    def tearDown(self):
        super(MetadataEntryTests, self).tearDown()
        self.request_context.pop()

    @patch.object(CFNotifier, 'notify')
    @patch.object(Elasticsearch, 'index')
    def test_insertEntry_newEntry_entryCreated(self, mock_es_index, mock_notifier):
        mock_es_index.return_value = {'created': True}
        response = self.client.put(
            self.TEST_ENTRY_URL,
            data=json.dumps(self.test_entry['_source']))
        self.assertEqual(201, response.status_code)
        mock_es_index.assert_called_with(**self.index_args)
        self.assertTrue(mock_notifier.called)

    @patch.object(CFNotifier, 'notify')
    @patch.object(Elasticsearch, 'index')
    def test_insertEntryNotAdmin_newEntry_entryCreated(self, mock_es_index, mock_notifier):
        mock_es_index.return_value = {'created': True}
        flask.g.is_admin = False
        flask.g.org_uuid_list = ["org02"]
        response = self.client.put(
            self.TEST_ENTRY_URL,
            data=json.dumps(self.test_entry['_source']))
        self.assertEqual(201, response.status_code)
        mock_es_index.assert_called_with(**self.index_args)
        self.assertTrue(mock_notifier.called)

    @patch.object(CFNotifier, 'notify')
    @patch.object(Elasticsearch, 'index')
    def test_insertEntry_entryExists_entryUpdated(self, mock_es_index, mock_notifier):
        mock_es_index.return_value = {'created': False}
        response = self.client.put(
            self.TEST_ENTRY_URL,
            data=json.dumps(self.test_entry['_source']))
        self.assertEqual(200, response.status_code)
        mock_es_index.assert_called_with(**self.index_args)
        self.assertTrue(mock_notifier.called)

    @patch.object(CFNotifier, 'notify')
    def test_insertEntry_malformedEntry_400Returned(self, mock_notifier):
        del self.test_entry['_source']['format']
        response = self.client.put(
            self.TEST_ENTRY_URL,
            data=json.dumps(self.test_entry['_source']))
        self.assertEqual(400, response.status_code)
        self.assertTrue(mock_notifier.called)

    @patch.object(Elasticsearch, 'get')
    def test_getEntryAdmin_entryExists_entryReturned(self, mock_es_get):
        mock_es_get.return_value = dict(self.test_entry)
        response = self.client.get(self.TEST_ENTRY_URL)
        self.assertEqual(200, response.status_code)
        mock_es_get.assert_called_with(**self.get_args)

    @patch.object(Elasticsearch, 'get')
    def test_getEntryNotAdmin_entryExists_entryReturned(self, mock_es_get):
        flask.g.is_admin = False
        flask.g.org_uuid_list = ["org02"]
        mock_es_get.return_value = dict(self.test_entry)
        response = self.client.get(self.TEST_ENTRY_URL)
        self.assertEqual(200, response.status_code)
        mock_es_get.assert_called_with(**self.get_args)

    @patch.object(Elasticsearch, 'get')
    def test_getEntry_entryNonExistent_404Returned(self, mock_es_get):
        mock_es_get.side_effect = NotFoundError
        response = self.client.get(self.TEST_ENTRY_URL)
        self.assertEqual(404, response.status_code)

    @patch.object(Elasticsearch, 'get')
    def test_getEntry_noIndexConnection_503Returned(self, mock_es_get):
        mock_es_get.side_effect = ConnectionError
        response = self.client.get(self.TEST_ENTRY_URL)
        self.assertEqual(503, response.status_code)

    @patch.object(CFNotifier, 'notify')
    @patch.object(Elasticsearch, 'get')
    @patch.object(DataSetRemover, 'delete')
    def test_deleteEntry_entryExists_200returned(self, mock_dataset_delete, mock_get_method, mock_notifier):
        mock_dataset_delete.return_value = None
        response = self.client.delete(
            self.TEST_ENTRY_URL,
            headers={'Authorization': self.AUTH_TOKEN})
        self.assertEqual(200, response.status_code)
        mock_dataset_delete.assert_called_with(self.get_args['id'], self.AUTH_TOKEN)


    @patch.object(CFNotifier, 'notify')
    @patch.object(DataSetRemover, 'delete')
    @patch.object(Elasticsearch, 'get')
    def test_deleteEntryNotAdmin_entryExists_200returned(self, mock_es_get, mock_dataset_delete, mock_notifier):
        flask.g.is_admin = False
        flask.g.org_uuid_list = ["org02"]
        mock_es_get.return_value = {'_source': {'orgUUID': 'org02'}}
        mock_dataset_delete.return_value = None
        response = self.client.delete(
            self.TEST_ENTRY_URL,
            headers={'Authorization': self.AUTH_TOKEN})
        self.assertEqual(200, response.status_code)
        mock_dataset_delete.assert_called_with(self.get_args['id'], self.AUTH_TOKEN)
        self.assertTrue(mock_notifier.called)

    @patch.object(CFNotifier, 'notify')
    @patch.object(Elasticsearch, 'get')
    def test_deleteEntry_tokenNotFound_404Returned(self,  mock_get_method, mock_notifier):
        response = self.client.delete(self.TEST_ENTRY_URL, None)
        self.assertEqual(401, response.status_code)


    @patch.object(CFNotifier, 'notify')
    @patch.object(Elasticsearch, 'get')
    @patch.object(DataSetRemover, 'delete')
    @data((NotFoundError, 404),
          (ConnectionError, 503))
    @unpack
    def test_deleteEntry_elasticEntryErroneous_ErrorReturned(
            self,
            side_effect,
            status_code,
            mock_dataset_delete, mock_get_method, mock_notifier):
        mock_dataset_delete.side_effect = side_effect
        response = self.client.delete(
            self.TEST_ENTRY_URL,
            headers={'Authorization': self.AUTH_TOKEN})
        self.assertEqual(status_code, response.status_code)
        self.assertTrue(mock_notifier.called)

    @patch.object(CFNotifier, 'notify')
    @patch.object(Elasticsearch, 'get')
    @patch.object(Elasticsearch, 'update')
    @patch.object(DataSetRemover, 'delete_public_table_from_dataset_publisher')
    @patch.object(MetadataEntryResource, '_get_token_from_request', return_value=AUTH_TOKEN)
    def test_changeField_dataSetExists_FieldUpdated(self, mock_get_token, mock_dataset_remover, mock_update_method, mock_get_method, mock_notifier):
        proper_update_request = {'doc': {self.IS_PUBLIC_FIELD: self.test_entry_index['_source'][self.IS_PUBLIC_FIELD]}}
        response = self.client.post(
            self.TEST_ENTRY_URL,
            data=json.dumps(self.TEST_BODY))
        self.assertEqual(200, response.status_code)
        mock_dataset_remover.assert_called_with(
            self.TEST_DATA_SET_ID,
            self.AUTH_TOKEN
        )
        mock_update_method.assert_called_with(
            index=self._config.elastic.elastic_index,
            doc_type=self._config.elastic.elastic_metadata_type,
            id=self.TEST_DATA_SET_ID,
            body=proper_update_request)
        self.assertTrue(mock_notifier.called)

    @patch.object(CFNotifier, 'notify')
    @patch.object(Elasticsearch, 'get')
    @patch.object(Elasticsearch, 'update')
    @patch.object(DataSetRemover, 'delete_public_table_from_dataset_publisher')
    @patch.object(MetadataEntryResource, '_get_token_from_request', return_value=AUTH_TOKEN)
    def test_change_noDataSet_404Returned(self, mock_get_token, mock_dataset_remover, mock_update_method, mock_get_method, mock_notifier):
        mock_update_method.side_effect = NotFoundError()
        response = self.client.post(
            self.TEST_ENTRY_URL,
            data=json.dumps(self.TEST_BODY))
        self.assertEqual(404, response.status_code)
        self.assertTrue(mock_notifier.called)
        mock_dataset_remover.assert_called_with(
            self.TEST_DATA_SET_ID,
            self.AUTH_TOKEN
        )

    @patch.object(CFNotifier, 'notify')
    @patch.object(Elasticsearch, 'get')
    @patch.object(Elasticsearch, 'update')
    @patch.object(DataSetRemover, 'delete_public_table_from_dataset_publisher')
    def test_changeField_internalError_503Returned(self, mock_dataset_remover, mock_update_method, mock_get_method, mock_notifier):
        mock_update_method.side_effect = ConnectionError()
        response = self.client.post(
            self.TEST_ENTRY_URL,
            data=json.dumps(self.TEST_BODY))
        self.assertEqual(503, response.status_code)
        self.assertTrue(mock_notifier.called)

    def test_changeField_badInput_400Returned(self):
        response = self.client.post(
            self.TEST_ENTRY_URL,
            data=json.dumps({'other_org_uuid': 'something'}))
        self.assertEqual(400, response.status_code)


class MetadataEntryTransformationTests(DataCatalogTestCase):
    TEST_DATA_SET_ID = 'whatever-id'
    EXAMPLE_CATEGORIES = {'health', 'finance'}
    CREATION_TIME_FIELD = 'creationTime'
    CATEGORY_FIELD = 'category'
    TARGET_URI_FIELD = 'targetUri'
    ORG_UUID_FIELD = 'orgUUID'

    def setUp(self):
        super(MetadataEntryTransformationTests, self).setUp()
        self.org_uuid = 'org01'
        self.test_entry = {
            self.CATEGORY_FIELD: 'health',
            'dataSample': 'some sample',
            'format': 'csv',
            'recordCount': 13,
            'size': 99999,
            'sourceUri': 'some uri',
            self.TARGET_URI_FIELD: 'hdfs://6.6.6.6:8200/borker/long-long-hash/9213-154b-a0b9/000000_1',
            'title': 'a great title',
            'isPublic': True,
            self.CREATION_TIME_FIELD: '2015-02-13T13:00:00',
            self.ORG_UUID_FIELD: self.org_uuid
        }

        self.test_entry_index = {
            self.CATEGORY_FIELD: 'health',
            'dataSample': 'some sample',
            'format': 'csv',
            'recordCount': 13,
            'size': 99999,
            'sourceUri': 'some uri',
            self.TARGET_URI_FIELD: 'hdfs://6.6.6.6:8200/borker/long-long-hash/9213-154b-a0b9/000000_1',
            'title': 'a great title',
            'isPublic': True,
            self.CREATION_TIME_FIELD: '2015-02-13T13:00:00',
            self.ORG_UUID_FIELD: self.org_uuid
        }
        self.parser = MetadataIndexingTransformer()

    def test_entryTransformation_validEntry_entryTransformed(self):
        self.parser.transform(self.test_entry)

        self.assertDictEqual(
            self.test_entry_index,
            self.test_entry)

    def test_entryTransformation_invalidEntryURIs_raisesInvalidEntryError(self):
        def check_raises_for_url(url):
            self.test_entry[self.TARGET_URI_FIELD] = url
            self.assertRaises(InvalidEntryError, self.parser.transform, self.test_entry)

        check_raises_for_url('//onet.pl/')
        check_raises_for_url('hdfs://onet.pl/')
        check_raises_for_url('http://')
        check_raises_for_url('some_path')

    def test_entryTransformation_invalidEntryMissingField_raisesInvalidEntryError(self):
        del self.test_entry['dataSample']
        self.assertRaises(InvalidEntryError, self.parser.transform, self.test_entry)

    def test_entryTransformation_missingDate_dateCreated(self):
        del self.test_entry[self.CREATION_TIME_FIELD]
        self.parser.transform(self.test_entry)
        self.assertTrue(self.test_entry.__contains__(self.CREATION_TIME_FIELD))

if __name__ == '__main__':
    unittest.main()