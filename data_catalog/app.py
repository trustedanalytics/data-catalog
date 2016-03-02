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
from elasticsearch import Elasticsearch
import elasticsearch.exceptions

from data_catalog.auth import Security
from data_catalog.elastic_admin import ElasticSearchAdminResource
from data_catalog.configuration import DCConfig
from data_catalog.metadata_entry import MetadataEntryResource
from data_catalog.search import DataSetSearchResource
from data_catalog.dataset_count import DataSetCountResource
from data_catalog.api_doc import ApiDoc


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

        self._log.exception("Exception with timestamp (%d) occured: %s", timestamp, e)

        response = {
            'message'   : message,
            'status'    : code,
            'timestamp' : timestamp
        }
        return self.make_response(response, code)


class PositiveMessageFilter(logging.Filter):

    """
    Logging filter that allows only positive messages to pass
    """

    @staticmethod
    def filter(record):
        return record.levelno not in (logging.WARNING, logging.ERROR)


def _prepare_environment(config):
    """
    Prepares ElasticSearch index for work if it's not yet ready.
    :param `DCConfig` config:
    """
    elastic_search = Elasticsearch('{}:{}'.format(
        config.elastic.elastic_hostname,
        config.elastic.elastic_port))
    try:
        elastic_search.indices.create(
            index=config.elastic.elastic_index,
            body=config.elastic.metadata_index_setup)
    except elasticsearch.exceptions.RequestError as ex:
        # Multiple workers can be created at the same time and there's no way
        # to tell ElasticSearch to create index only if it's not already created,
        # so we need to attempt to create it and ignore the error that it throws
        # when attemting to create an existing index.
        if 'IndexAlreadyExists' in ex.args[1]:
            print('400 caused by "index already created" - no need to panic.')
        else:
            raise ex
    except elasticsearch.exceptions.TransportError:
        print("Can't start because of no connection to ElasticSearch.")
        raise


def _configure_logging(config):
    """
    Configures proper logging for the application.
    :param `DCConfig` config:
    """
    log_formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')

    positive_handler = logging.StreamHandler(sys.stdout)
    positive_handler.addFilter(PositiveMessageFilter())
    positive_handler.setFormatter(log_formatter)

    negative_handler = logging.StreamHandler(sys.stderr)
    negative_handler.setLevel(logging.WARNING)
    negative_handler.setFormatter(log_formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(config.log_level)
    root_logger.addHandler(positive_handler)
    root_logger.addHandler(negative_handler)


def _create_app(config):
    app = Flask(__name__)
    api = ExceptionHandlingApi(app)
    api_doc_route = '/api-docs'

    api.add_resource(DataSetSearchResource, config.app_base_path)
    api.add_resource(ApiDoc, api_doc_route)
    api.add_resource(MetadataEntryResource, config.app_base_path + '/<entry_id>')
    api.add_resource(DataSetCountResource, config.app_base_path + '/count')
    api.add_resource(ElasticSearchAdminResource, config.app_base_path + '/admin/elastic')

    security = Security(auth_exceptions=[api_doc_route])
    app.before_request(security.authenticate)

    return app


def get_app():
    """
    To be used by the WSGI server.
    """
    config = DCConfig()
    _configure_logging(config)
    _prepare_environment(config)
    return _create_app(config)
