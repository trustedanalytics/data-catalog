Elastic search migration tool
================================

This script is using data-catalog REST interface to fetch, delete and insert data from elastic search. It primary use case is data migration.It can be also used to retrive, change data/index and then insert to elastic.

## Usage

``` python elastic_migrate_tool.py [-h] (-fetch | -delete | -insert) token [base_url] ```

## Parameters:

Params: -fetch, -delete and -insert cannot be used together.

* token: OAUTH token. For delete and insert it must have admin privileges
* base_url: base URL for datacatalog service. Default: http://localhost:5000
* -h, --help: show help message and exit
* -fetch: fetch data from elastic search. Retrived data is save in working directory in file: data_input.json
* -delete: delete data by removing elastic search index
* -insert: insert data from file. Expected file name is: data_input.json and it should be found in working directory

