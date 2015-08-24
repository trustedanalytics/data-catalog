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

# pylint: disable=invalid-name


import logging
import sys

from time import time
from flask import Flask
from flask_restful import Api
from flask_restful_swagger import swagger
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError

from data_catalog.auth import Security
from data_catalog.elastic_admin import ElasticSearchAdminResource
from data_catalog.configuration import DCConfig
from data_catalog.metadata_entry import MetadataEntryResource
from data_catalog.search import DataSetSearchResource
from data_catalog.dataset_count import DataSetCountResource
from data_catalog.version import VERSION


class ExceptionHandlingApi(Api):

    """
    Overrides standard error handler that Flask API provides
    """

    def __init__(self, wsgi_app):
        self._log = logging.getLogger(type(self).__name__)
        super(ExceptionHandlingApi, self).__init__(wsgi_app)

    def handle_error(self, e):
        DEFAULT_ERROR_STATUS = 500

        code = getattr(e, 'code', DEFAULT_ERROR_STATUS)
        #converting to msecimport
        timestamp = int(time()*1000)

        message = None
        if hasattr(e, 'data'):
            message = e.data.get('message')
        if message is None:
            message = getattr(e, 'description', 'Internal Server Error')

        self._log.exception("Exception with timestamp (%d) occured: %s" , timestamp, e)

        response = {
            'message'   : message,
            'status'    : code,
            'timestamp' : timestamp
        }
        return self.make_response(response, code)


_CONFIG = DCConfig()

app = Flask(__name__)
# our RESTful API wrapped with Swagger documentation
api = swagger.docs(
    ExceptionHandlingApi(app),
    apiVersion=VERSION,
    description='Data Catalog - enables search, retrieval and storage of metadata '
                'describing data sets. ')

api.add_resource(DataSetSearchResource, _CONFIG.app_base_path)
api.add_resource(MetadataEntryResource, _CONFIG.app_base_path + '/<entry_id>')
api.add_resource(DataSetCountResource, _CONFIG.app_base_path + '/count')
api.add_resource(ElasticSearchAdminResource, _CONFIG.app_base_path + '/admin/elastic')

ignore_for_auth = ['/api/spec']
security = Security(ignore_for_auth)
app.before_request(security.authenticate)


def prepare_environment():
    """Prepares ElasticSearch index for work if it's not yet ready."""
    elastic_search = Elasticsearch('{}:{}'.format(
        _CONFIG.elastic.elastic_hostname,
        _CONFIG.elastic.elastic_port))
    try:
        if not elastic_search.indices.exists(_CONFIG.elastic.elastic_index):
            elastic_search.indices.create(
                index=_CONFIG.elastic.elastic_index,
                body=_CONFIG.elastic.metadata_index_setup)
    except ConnectionError:
        sys.exit("Can't start because of no connection to ElasticSearch.")


class PositiveMessageFilter(logging.Filter):

    """
    Logging filter that allows only positive messages to pass
    """

    @staticmethod
    def filter(record):
        return record.levelno not in (logging.WARNING, logging.ERROR)


def configure_logging():
    log_formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')

    positive_handler = logging.StreamHandler(sys.stdout)
    positive_handler.addFilter(PositiveMessageFilter())
    positive_handler.setFormatter(log_formatter)

    negative_handler = logging.StreamHandler(sys.stderr)
    negative_handler.setLevel(logging.WARNING)
    negative_handler.setFormatter(log_formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(_CONFIG.log_level)
    root_logger.addHandler(positive_handler)
    root_logger.addHandler(negative_handler)


if __name__ == "__main__":
    configure_logging()
    prepare_environment()
    app.run(
        host='0.0.0.0',
        port=_CONFIG.app_port,
        debug=_CONFIG.log_level == 'DEBUG',
        use_reloader=False)
