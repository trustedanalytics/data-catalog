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

import logging

from elasticsearch import Elasticsearch

from flask_restful import Resource
from data_catalog.configuration import DCConfig


class DataCatalogResource(Resource):

    """
    RESTful resource with Data Catalog configuration.
    Should be used as base for other resources.
    """

    def __init__(self):
        super(DataCatalogResource, self).__init__()
        self._config = DCConfig()
        self._log = logging.getLogger(type(self).__name__)


class DataCatalogModel(object):

    """
    Base for the application's model classes.
    """

    def __init__(self):
        self._config = DCConfig()
        self._log = logging.getLogger(type(self).__name__)
        self._elastic_search = Elasticsearch(
            '{}:{}'.format(self._config.elastic.elastic_hostname,
                           self._config.elastic.elastic_port))

    def _get_entry(self, entry_id):
        """
        shortcut to ElasticSearch.get function
        Standard elastic (index/doc_type) params are added
        :param entry_id: elastic search id
        :raises NotFoundError: entry not found in Elastic Search
        :raises ConnectionError: problem with connecting to Elastic Search
        :return: elastic search structure
        """
        return self._elastic_search.get(
            index=self._config.elastic.elastic_index,
            doc_type=self._config.elastic.elastic_metadata_type,
            id=entry_id)

    def _delete_entry(self, entry_id):
        """
        shortcut to ElasticSearch.delete function
        data flush is performed after delete
        Standard elastic (index/doc_type) params are added
        :param entry_id: elastic search id
        :raises NotFoundError: entry not found in Elastic Search
        :raises ConnectionError: problem with connecting to Elastic Search
        :rtype: None

        """
        self._elastic_search.delete(
            index=self._config.elastic.elastic_index,
            doc_type=self._config.elastic.elastic_metadata_type,
            id=entry_id)

        # flushing data - so immediate searches are aware of change
        self._elastic_search.indices.flush()



