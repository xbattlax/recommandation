import pandas as pd
import requests
from elasticsearch import Elasticsearch
import json
import numpy as np
user = 1

response = requests.get('http://localhost:9200/goodreads_user/_doc/' + str(user))

# get _doc user
user = response.json()['_source']

# print dimensions of vector
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

res = es.knn_search(index='goodreads_user',
                    knn={"field": "similarity-vector", "query_vector": user['similarity-vector'], "k": 5, "num_candidates": 10})

users = []
user_books = {}
for hit in res['hits']['hits']:
    if hit['_id'] != user:
        books ={}
        ratings = json.loads(hit['_source']['rating'])
        # convert to object
        for book in ratings:
            books[book['book_id']] = book['rating']
            user_books[book['book_id']] = book['rating']

        users.append((hit['_id'], hit['_score'], books))

all_books = set(book_id for book_id in user_books.keys())
unread_books = set(book_id for book_id in all_books if user_books[book_id] == 0)

user_book = json.loads(user['rating'])

set_book = set()


list_of_set = []

for book in user_book:
    responseB = requests.get('http://localhost:9200/goodreads_item/_doc/' + str(book['book_id']))
    vector = responseB.json()['_source']['similarity-vector']
    res = es.knn_search(index='goodreads_item',
                    knn={"field": "similarity-vector", "query_vector": vector, "k": 5, "num_candidates": 1000})
    for hit in res['hits']['hits']:
        if hit['_id'] != book['book_id']:
            set_book.add(int(hit['_id']))
    list_of_set.append(set_book)


# combine all set



set_book = set.union(*list_of_set)

inter = set.intersection(set_book, unread_books)

print(inter)

