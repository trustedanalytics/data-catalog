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

"""
Tests for the app initialization code in app.py.
"""

import elasticsearch
from elasticsearch.exceptions import RequestError
import mock
import pytest

import base_test

import data_catalog.app as app
from data_catalog.configuration import DCConfig


@pytest.yield_fixture
def mock_es_create():
    with mock.patch.object(elasticsearch.client.indices.IndicesClient, 'create') as mock_create:
        yield mock_create


@pytest.fixture
def test_config(fake_env_vars):
    return DCConfig()


def test_first_prepare_environment(mock_es_create, test_config):
    app._prepare_environment(test_config)

    mock_es_create.assert_called_with(
        index=test_config.elastic.elastic_index,
        body=test_config.elastic.metadata_index_setup)


def test_subsequent_prepare_environment(mock_es_create, test_config):
    mock_es_create.side_effect = RequestError(400, 'IndexAlreadyExists errorito')
    app._prepare_environment(test_config)


def test_prepare_environment_err(mock_es_create, test_config):
    mock_es_create.side_effect = RequestError(123, 'Some other error')
    with pytest.raises(RequestError):
        app._prepare_environment(test_config)

