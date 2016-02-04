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

from __future__ import print_function

import json
import sys
import urlparse
import argparse

from jwt.utils import base64url_decode
import requests
from requests.auth import AuthBase

FETCH_URL = "rest/datasets"
DELETE_URL = "rest/datasets/admin/elastic"
INSERT_URL = "rest/datasets/admin/elastic"
LOCALHOST_URL = "http://localhost:5000"
DEFAULT_FILE = "data_input.json"


class Authorization(AuthBase):
    def __init__(self, token):
        self.token = token

    def __call__(self, request):
        request.headers['Authorization'] = self.token
        return request


def fetch_data(base_url, token):
    full_path = urlparse.urljoin(base_url, FETCH_URL)
    print('Calling URL', full_path)
    r = requests.get(full_path, auth=Authorization(token))
    r.raise_for_status()
    data = json.loads(r.content)
    
    if data and "hits" in data:
        query = json.dumps({"size": data["total"]})
        r = requests.get(full_path, params={'query': query}, auth=Authorization(token))

        data = json.loads(r.content)
        new_data = data["hits"]

        f = open(DEFAULT_FILE, 'w')
        f.write(json.dumps(new_data, sort_keys=True, indent=2, separators=(',', ': ')))
        f.close()
        print("data fetched and saved in:", DEFAULT_FILE)
    else:
        print("no data or error recived:", r.status_code, data)


def delete_index(base_url, token):
    full_path = urlparse.urljoin(base_url, DELETE_URL)
    r = requests.delete(full_path, auth=Authorization(token))
    if (r.status_code == 200):
        print("indexes deleted")
    else:
        print("problem with delete: ", r.status_code, r.text)

def insert_data(base_url, token):

    f = open(DEFAULT_FILE, 'r')
    data = f.read()
    full_path = urlparse.urljoin(base_url, INSERT_URL)
    r = requests.put(full_path, auth=Authorization(token), data=data)
    if (r.status_code == 200):
        print("data inserted")
    else:
        print("problem with insert:", r.status_code, r.text)

class CheckUrlSchemeAction(argparse.Action):
    def __call__(self, parser, namespace, base_url, option_string=None):
        if not base_url.startswith("http"):
            base_url = "http://"+base_url
        setattr(namespace, self.dest, base_url)


def parse_args():
    parser = argparse.ArgumentParser(description='This script fetch/delete and insert data from elastic search used in datacatalog service.')

    group_list = parser.add_mutually_exclusive_group(required=True)
    group_list.add_argument('-fetch', help='fetch data from elastic search. Retrived data is save in working directory in file: ' + DEFAULT_FILE, action='store_true')
    group_list.add_argument('-delete', help='delete data by removing elastic search index', action='store_true')
    group_list.add_argument('-insert', help='insert data from file. Expected file name is: '+DEFAULT_FILE+' and it should be found in working directory', action='store_true')

    parser.add_argument('token',help="OAUTH token. For delete and insert it must have admin privileges")
    parser.add_argument('base_url', nargs='?', default=LOCALHOST_URL, action=CheckUrlSchemeAction, help="base URL for datacatalog service. Default: %(default)s")

    return parser.parse_args()


def is_admin(user_token):
    """
    :param str user_token: User's OAuth 2 token payload (containing "bearer" prefix).
    :return: True if the user is an admin.
    :rtype: bool
    """
    print('Provided token:', user_token)
    # get token without "bearer"
    token = user_token.split()[1]
    # take the middle part with payload
    token_payload_based = token.split('.')[1]
    token_payload_str = base64url_decode(token_payload_based.encode()).decode()
    token_payload = json.loads(token_payload_str)
    return 'console.admin' in token_payload['scope']


if __name__ == '__main__':
    args = parse_args()

    if not is_admin(args.token):
        sys.exit('ERROR! You must have admin privilages (console.admin scope) to use this tool.')            
    
    if args.fetch:
        fetch_data(args.base_url, args.token)
    elif args.delete:
        delete_index(args.base_url, args.token)
    elif args.insert:
        insert_data(args.base_url, args.token)
    else:
        print("This shouldn't happen...")

