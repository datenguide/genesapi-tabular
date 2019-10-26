import os


ELASTIC_HOST = os.getenv('ELASTIC_HOST', 'http://localhost:9200')
ELASTIC_INDEX = os.getenv('ELASTIC_INDEX', 'genesapi')
ELASTIC_CACHE_INDEX = os.getenv('ELASTIC_CACHE_INDEX', 'genesapi-tabular-cache--%s' % ELASTIC_INDEX)
ELASTIC_AUTH = os.getenv('ELASTIC_AUTH')
STORAGE_NAME = os.getenv('STORAGE_NAME', 'Regionalstatistik')
DOCS_FILE = './README.md'
SCHEMA_URL = 'https://data.genesapi.org/regionalstatistik/schema.json'
NAMES_URL = 'https://data.genesapi.org/regionalstatistik/names.json'
GENESAPI_TABULAR_STATIC = 'https://static.tabular.genesapi.org'
