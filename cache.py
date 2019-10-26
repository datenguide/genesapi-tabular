from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError

from settings import ELASTIC_CACHE_INDEX, ELASTIC_HOST, ELASTIC_AUTH


class ElasticsearchBackend:
    def __init__(self):
        self.client = Elasticsearch([ELASTIC_HOST], http_auth=ELASTIC_AUTH)

    def get(self, id_):
        try:
            return self.client.get_source(index=ELASTIC_CACHE_INDEX, id=id_)
        except NotFoundError:
            return

    def set(self, id_, body):
        body['created'] = datetime.now().isoformat()
        return self.client.index(index=ELASTIC_CACHE_INDEX, id=id_, body=body)


class BaseCache:
    def __init__(self, backend):
        self.backend = backend

    def get(self, id_):
        return self.backend.get(id_)

    def set(self, id_, body):
        return self.backend.set(id_, body)


Cache = BaseCache(ElasticsearchBackend())
