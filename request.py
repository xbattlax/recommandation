import pandas as pd
import requests
from elasticsearch import Elasticsearch
import numpy as np
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
ds = pd.read_csv('goodreads_interactions_reduced.csv')

print(ds.head())
# make user in line and book in column
ds = ds.pivot(index='user_id', columns='book_id', values='rating')




print(ds.head())
# count

# get line of user
vector_user = ds.iloc[user].tolist()
vectors = [ds.iloc[int(user[0])].tolist() for user in users]

# lu par v mais pas vector_user
for j in range(len(vectors)):
    read_by_vector1 = np.where(vectors[j] != 0)
    read_by_vector2 = np.where(vector_user != 0)

    difference = np.setdiff1d(read_by_vector1, read_by_vector2)
    print(difference)
    res = [(i, vectors[j][i]*users[j][1]) for i in difference]
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
