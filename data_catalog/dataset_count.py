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

import flask

from elasticsearch import Elasticsearch

from data_catalog.bases import DataCatalogResource
from data_catalog.search import DataSetSearch


# TODO add tests
class DataSetCountResource(DataCatalogResource):

    """
    Shows how many data sets are currently in the index.
    """

    def __init__(self):
        super(DataSetCountResource, self).__init__()
        self._elastic_search = Elasticsearch(
            '{}:{}'.format(self._config.elastic.elastic_hostname,
                           self._config.elastic.elastic_port))
        self._search = DataSetSearch()

    def get(self):
        """
        Get the number of current data sets in the index per organisation.
        """
        args = flask.request.args
        params = self._search.get_params_from_request_args(args)

        return self._search.search({}, flask.g.org_uuid_list,
                                   params['dataset_filtering'],
                                   flask.g.is_admin)['total']
