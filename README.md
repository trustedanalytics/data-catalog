[![Dependency Status](https://gemnasium.com/trustedanalytics/data-catalog.svg)](https://gemnasium.com/trustedanalytics/data-catalog)
[![Code Climate](https://codeclimate.com/repos/55e2d6926956804af0000d98/badges/f9159de81b7021f7b648/gpa.svg)](https://codeclimate.com/repos/55e2d6926956804af0000d98/feed)

data-catalog
============

This service is a backend to the "Data Catalog" tab in Console.
It is used to store, retrieve and to search over metadata describing data sets downloaded into Trusted Analytics platform.

## Service dependencies
* ElasticSearch - metadata store backing
* Downloader (Trusted Analytics platform) - is called to delete the actual data of the data sets
* Dataset Publisher - is called to delete the Hive views of the data sets

## Basic handling

### Initial setup
* You need Python (of course).
* Install pip: `sudo apt-get install python-pip`
* Install Tox: `sudo pip install tox`

### Tests
* Be in `data-catalog` directory (project's source directory).
* Run: `tox` (first run will take long) or `tox -r` (if you have added something to requirements.txt)

### Configuration
Configuration is handled through environment variables. They can be set in the "env" section of the CF (Cloud Foundry) manifest.
Parameters:
* **LOG_LEVEL** - Application's logging level. Should be set to one of logging levels from Python's `logging` module (e.g. DEBUG, INFO, WARNING, ERROR, FATAL). DEBUG is the default one if the parameter is not set.

### Tools
There are few development tools to handle or setup data in data-catalog:
* [Local setup tool] (#local-development-tools)
* [Migration tool] (tools/ELASTIC_MIGRATE_README.md)

### Development

#### General
* **Everything should be done in a Python virtual environment (virtualenv).**
* To switch the command line to the project's virtualenv run `source .tox/py27/bin/activate`. Run `deactivate` to disable virtualenv.
* Downloading additional dependencies (libraries): `pip install <library_name>`
* Updating requirements file after adding libraries: `pip freeze -l > requirements.txt`

#### Local setup
1. [Install ElasticSearch] (https://www.elastic.co/downloads/elasticsearch) on your development machine.
1. Change the `cluster.name` property in ELASTIC_SEARCH_DIR/config/elasticsearch.yml to an unique name. This will prevent your instance of ES from automatically merging into a cluster with others on the local network.
1. Run ElasticSearch.
1. Download and run User Management app (available in the same Github organization as this).
1. Set a VCAP_SERVICES variable that would normally be set by CF.
```export VCAP_SERVICES='{"user-provided":[{"credentials":{"tokenKey":"http://uaa.example.com/token_key"},"tags":[],"name":"sso", "label":"user-provided"}]}'```
1. Running data-catalog service locally (first run prepares the index): `python -m data_catalog.app`
1. Additional: some functions require Downloader and Dataset Publisher apps (also from the same Github organization as this).

#### Local development tools
* Fill local ElasticSearch index with data (do after preparing the index): `python -m tools.local_index_setup fill`
* Generating other set of example metadata: `python -m tools.local_index_setup generate <entry_number>`
* To delete the index run: `python -m tools.local_index_setup delete`


### API documentation
* Documentation is interactive, generated using [Swagger] (http://swagger.io/).
* Run the application (described in "Development")
* Open http://localhost:5000/api/spec.html in your browser.

### Integration with PyCharm / IntelliJ with Python plugin
* Run `tox` in project's source folder to create a virtualenv.
* If you hava a new version of PyCharm/Idea you might need to remove `.tox` folder from exclusions in Python projects. Follow [this resolution] (https://youtrack.jetbrains.com/issue/PY-16141#comment=27-1015284).
* File -> New Project -> Python
* In "Project SDK" choose "New" -> "Add Local".
* Select Python executable in `.tox` directory created in your source folder (enable showing hidden files first).
* Skip ahead, then set "Project Location" to the source folder.
* Add new "Python tests (Unittests)" run configuration. Choose "All in folder" with the folder being &lt;source folder&gt;/tests. You can use this configuration to debug tests.
* Go to File -> Project Structure, then mark `.tox` folder as excluded.

## Advanced handling

### Test queries on local elastic search
* Install marvel plugin: bin/plugin -i elasticsearch/marvel/latest
* Go to the url: [http://localhost:9200/_plugin/marvel/sense/index.html] (http://localhost:9200/_plugin/marvel/sense/index.html)
* Once you fill your elasticsearch index with example data (see: Development section), you can test it
* List of indexes: `GET _cat/indices` (you will see also .marvel indexes produced by the plugin, to remove them you can run `DELETE .marvel*`)
* `GET trustedanalytics-meta/` lists the set up of the 'trustedanalytics-meta' index (mappings, settings, etc)
* Example of a query on chosen field:
```
GET trustedanalytics-meta/dataset/_search
{
  "query":{
    "match": {
      "dataSample": "something"
    }
  }
}
```
* Example of a fuzzy query:
```
GET trustedanalytics-meta/dataset/_search
{
  "query": {
    "match": {
      "title": {
        "query": "tneft",
        "fuzziness": 1
      }
    }
  }
}
```
* Example of a query to our custom analyzer (called uri_analyzer)
`GET trustedanalytics-meta/_analyze?analyzer=uri_analyzer&text='http:/some-addres.example.com/dataset'`

### Bumpversion
Bumpversion tool is used for doing versioning tasks related to releases like doing git commits (but not pushing) and tags.

**Possible actions:**
* Release: `bumpversion patch` and `git push`
* Update minor/major version: `bumpversion minor` or `bumpversion major` followed by `git push`

**Notable things:**
* Configuration is in `.bumpversion.cfg` file.
* Version info is updated in three files: `.bumpversion.cfg`, `data_catalog/version.py`, `manifest.yml`
* Version tags are created automatically.
* Commits are done automatically.
