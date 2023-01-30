import pandas as pd
import requests
from elasticsearch import Elasticsearch

user = 1

response = requests.get('http://localhost:9200/goodreads/_doc/' + str(user))

vector = response.json()['_source']['similarity-vector']

# print dimensions of vector
print(len(vector))
# es
es = Elasticsearch(
    [
        {
            'host': "localhost",
            'port': 9200,
            'scheme': "http"
        }
    ],
    http_auth=("elastic", "KgSU+VEYRvPvW09c9czx")
)

res = es.knn_search(index='goodreads',
                    knn={"field": "similarity-vector", "query_vector": vector, "k": 5, "num_candidates": 10})

users = []

for hit in res['hits']['hits']:
    if hit['_id'] != user:
        users.append((hit['_id'], hit['_score']))



# load goodreads dataset only for user in user list
ds = pd.read_csv('ratings.csv')
ds1 = ds[ds['user_id' == user]]
ds2 = ds[ds['user_id'].isin(users)]

# interate over ds2 and find a book that user1 has not read
for index, row in ds2.iterrows():
    if row['book_id'] not in ds1['book_id']:
        print(row['book_id'])
        break

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
