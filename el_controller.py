from elasticsearch import Elasticsearch, helpers

es = 0


def init(host, port):
    global es
    es = Elasticsearch([{'host': host, 'port': port, 'timeout': 300}])


def search(index, query, size, q_json):
    res = es.search(size=size, index=index, body=q_json)
    return res


def bulk_action(actions):
    helpers.bulk(es, actions)


def delete_index(index_name):
    es.indices.delete(index=index_name, ignore=[400, 404])


def create_index(index_name, index_settings):
    es.indices.create(index=index_name, body=index_settings)
