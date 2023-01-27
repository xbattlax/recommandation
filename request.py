import requests
from elasticsearch import Elasticsearch

response = requests.get('http://localhost:9200/goodreads/_doc/1')

vector = response.json()['_source']['similarity']

# print dimensions of vector
print(len(vector))
# es
es = Elasticsearch(
    [
        {
            'host':"localhost",
            'port':9200,
            'scheme': "http"
        }
    ],
    http_auth=("elastic", "KgSU+VEYRvPvW09c9czx")
)

res = es.knn_search(index='goodreads', knn={"field": "similarity", "query_vector": vector, "k": 5, "num_candidates": 10})

print(res)

"""
headers = {
    'Content-Type': 'application/json',
}

json_data = {
    'knn': {
        'field': 'similarity',
        'query_vector': vector,
        'k': 10,
        'num_candidates': 10,
    },
    'fields': [
        '_id',
    ],
}

response = requests.post('http://localhost:9200/goodreads/_search?pretty', headers=headers, json=json_data)

print(response.json())"""