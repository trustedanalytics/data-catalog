#
# Copyright (c) 2016 Intel Corporation
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

import os
import re
import sys

HEADERS = [
"""#
# Copyright (c) 2016 Intel Corporation
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
""",
"""#
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
]

HEADER_LENGTHS = {header: len(header.splitlines()) for header in HEADERS}
MAX_HEADER_LENGTH = max(HEADER_LENGTHS.values())


def check_file_headers(root_path, extensions, path_exceptions):
    """Checks if all files under the given root_path with the given extensions have proper license
    headers.

    Args:
        root_path (str): Path in which headers will be scanned. Will scan all subdirectories
            recursively.
        extensions (set[str]): Only the files with one of theese extensions (eg. "py") will be
            scanned.
        path_exceptions (list[str]): Which paths not to scan.
    Returns:
        bool: True if all scanned files have proper licenses. False otherwise.
    """
    path_regexes = ['(?:^{})'.format(path) for path in path_exceptions]
    regex = re.compile('|'.join(path_regexes))

    for path, _, file_names in os.walk(root_path):
        if regex.match(path):
            continue

        filtered_files = [file_name for file_name in file_names
                          if os.path.splitext(file_name)[1] in extensions]
        for file_path in [os.path.join(path, file_name) for file_name in filtered_files]:
            if not check_license_header(file_path):
                return False
    return True


def check_license_header(file_path):
    match_found = False
    try:
        with open(file_path) as checked_file:
            read_lines = [checked_file.readline() for _ in range(MAX_HEADER_LENGTH)]
            for header, num_lines in HEADER_LENGTHS.items():
                if header == ''.join(read_lines[:num_lines]):
                    match_found = True
                    raise Exception('We need to get out of those loops.')
    except Exception:
        pass

    if match_found:
        return True
    else:
        print('Wrong header in file: {}'.format(file_path))
        return False


if __name__ == '__main__':
    path_exceptions = ['./' + path for path in sys.argv[1].split(',')]
    extensions = {'.' + extension for extension in sys.argv[2].split(',')}

    if not check_file_headers('.', extensions, path_exceptions):
        sys.exit(1)
