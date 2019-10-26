import markdown
from flask import Flask, render_template, request, Response
from urllib.parse import urlparse

from cache import Cache
from elastic import ElasticQuery
from query import Query
from settings import DOCS_FILE
from table import Table


app = Flask(__name__)


@app.route('/docs/')
def docs():
    return render_template('docs.html', content=markdown.markdownFromFile(input=DOCS_FILE))


@app.route('/')
def api():
    if not request.args:
        return 'no url GET query'
    # we use the raw url GET query to parse instead of Flask`s built-in `request.args.get()`
    # to make the parsing independent from Flask (see `query.py`)
    q = Query(urlparse(request.url).query)

    # we use elasticsearch as a cache backend where we store raw text strings
    cache_hit = Cache.get(q.key)
    if cache_hit:
        return Response(cache_hit['content'], mimetype=cache_hit['mimetype'])

    # try to get the base table (no format/transform) from cache:
    base_data = Cache.get(q.data_key)
    if base_data:
        table = Table.from_base(base_data, q)

    else:
        # nothing in cache, so create the table
        es = ElasticQuery(q.cleaned_data)
        table = Table(es.facts, q)

    if 'debug' in request.args:
        return {
            'data': q.cleaned_data,
            'query_body': es.body,
            'table': table.formats
        }

    # store in cache for later use
    Cache.set(q.key, table.serialize())
    if base_data is None:
        Cache.set(q.data_key, table.serialize_base())

    return Response(table.rendered(), mimetype=table.mimetype)
