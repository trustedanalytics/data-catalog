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
import os.path
import random
import sys
import time
from datetime import datetime

from data_catalog.metadata_entry import IndexedMetadataEntry
from data_catalog.configuration import DCConfig
from elasticsearch import Elasticsearch


SETUP_FILLED_COMMAND = 'fill'
DELETE_INDEX_COMMAND = 'delete'
GENERATE_DATA_COMMAND = 'generate'

CATEGORIES = ['agriculture', 'business', 'consumer', 'education', 'energy', 'finance', 'health',
              'science']
CATEGORIES_OBJ = {'categories': CATEGORIES}

CONFIG = DCConfig()
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
EXAMPLE_METADATA_FILE = os.path.join(SCRIPT_DIR, 'example_metadata.json')


def print_help():
    print(
        '''To fill the local ElasticSearch index with test metadata entries run:
    {0} {1}

To delete the index run:
    {0} {2}

To generate new example metadata run:
    {0} {3} <example-entry-number>'''.format(__file__,
                                   SETUP_FILLED_COMMAND,
                                   DELETE_INDEX_COMMAND,
                                   GENERATE_DATA_COMMAND)
    )


elastic_search = Elasticsearch()


def delete_index():
    print('Deleting the ElasticSearch index.')
    elastic_search.indices.delete(CONFIG.elastic.elastic_index, ignore=404)
    print('Done.')


def setup_filled():
    print('Filling the index with test metadata.')
    with open(EXAMPLE_METADATA_FILE) as metadata_file:
        metadata_entries = json.loads(metadata_file.read())
    for number, entry in enumerate(metadata_entries):
        elastic_search.index(
            CONFIG.elastic.elastic_index,
            CONFIG.elastic.elastic_metadata_type,
            entry,
            number)

    print('Done.')


def generate_example_metadata(entry_number):
    print('Generating {} random entries in file {}'.format(entry_number, EXAMPLE_METADATA_FILE))

    def get_random_ISO8601_date():
        current_timestamp = int(time.time())
        seconds_in_year = 3600 * 24 * 365
        past_timestamp = current_timestamp - (2*seconds_in_year)
        random_timestamp = random.randint(past_timestamp, current_timestamp)

        random_date = str(datetime.fromtimestamp(random_timestamp))
        return 'T'.join(random_date.split())

    DATA_SAMPLE = 'ID,Something,OtherThing'
    titles = ['Zebras in Africa', 'Power usage in Mordor', 'Killings on the Moon',
              'I know where you live', 'All your base are belong to us',
              'Bike theft by invisible people', 'Werewolf sightings in 2015',
              'Teenage drama and vampirism occurences', 'North Dakota Voodoo dolls usage',
              'Tanks vs knifes - a cautionary tale', "I just love my CSV's"]
    random.shuffle(titles)
    org_uuids = ['org01', 'org02', 'org03']
    entries = []
    for i in range(entry_number):
        entry = {
            IndexedMetadataEntry.CATEGORY_FIELD: CATEGORIES[i % len(CATEGORIES)],
            'dataSample': DATA_SAMPLE,
            'format': 'CSV',
            'recordCount': random.randint(10, 100000),
            'size': random.randint(1000, 1000000000),
            'sourceUri': 'http://some-addres.example.com/dataset',
            'targetUri': '/borker/long-long-hash/9213-154b-a0b9',
            'storeType': 'hdfs',
            'isPublic': random.choice([True, False]),
            'orgUUID': org_uuids[i % len(org_uuids)],
            IndexedMetadataEntry.TITLE_FIELD: titles[i % len(titles)],
            IndexedMetadataEntry.CREATION_TIME_FIELD: get_random_ISO8601_date(),
        }
        entries.append(entry)
    with open(EXAMPLE_METADATA_FILE, 'w') as metadata_file:
        metadata_file.write(json.dumps(entries, sort_keys=True, indent=4))
    print('Done.')

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print_help()
        quit()

    command = sys.argv[1]
    if command == DELETE_INDEX_COMMAND:
        delete_index()
    elif command == SETUP_FILLED_COMMAND:
        setup_filled()
    elif command == GENERATE_DATA_COMMAND and sys.argv > 2:
        entry_number = int(sys.argv[2])
        generate_example_metadata(entry_number)
    else:
        print('No command: ' + command)
        print_help()
