
import pandas as pd
import requests
from elasticsearch import Elasticsearch
import json
import numpy as np
from collections import defaultdict

def calculate_mean_ratings(ratings):
    book_ratings = defaultdict(list)
    for user in ratings:
        for rating in user:
            book_ratings[rating['book_id']].append(rating['rating'])

    mean_ratings = {}
    for book_id, ratings in book_ratings.items():
        mean_ratings[book_id] = sum(ratings) / len(ratings)

    return mean_ratings
def find_similar_books(user):

    response = requests.get('http://localhost:9200/goodreads_user/_doc/' + str(user))

    # get _doc user
    try :
        user = response.json()['_source']
    except:
        return []

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
                        knn={"field": "similarity-vector", "query_vector": user['similarity-vector'], "k": 500, "num_candidates": 1000})

    users = []
    user_books = {}
    rat = []
    for hit in res['hits']['hits']:
        if hit['_id'] != user:
            books ={}
            ratings = json.loads(hit['_source']['rating'])
            rat.append(ratings)
            # convert to object
            for book in ratings:
                books[book['book_id']] = book['rating']
                user_books[book['book_id']] = book['rating']

            users.append((hit['_id'], hit['_score'], books))

    all_books = set(book_id for book_id in user_books.keys())
    unread_books = set(book_id for book_id in all_books if user_books[book_id] == 0)

    user_books = calculate_mean_ratings(rat)

    return dict(sorted(user_books.items(), key=lambda item: item[1], reverse=True))


def find_similar_hybrid(user):
    response = requests.get('http://localhost:9200/goodreads_user/_doc/' + str(user))

    # get _doc user
    try:
        user = response.json()['_source']
    except:
        return []
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
                        knn={"field": "similarity-vector", "query_vector": user['similarity-vector'], "k": 50,
                             "num_candidates": 100})

    users = []
    user_books = {}
    rat = []
    for hit in res['hits']['hits']:
        if hit['_id'] != user:
            books = {}
            ratings = json.loads(hit['_source']['rating'])
            rat.append(ratings)
            # convert to object
            for book in ratings:
                books[book['book_id']] = book['rating']
                user_books[book['book_id']] = book['rating']

            users.append((hit['_id'], hit['_score'], books))

    all_books = set(book_id for book_id in user_books.keys())
    unread_books = set(book_id for book_id in all_books if user_books[book_id] == 0)

    user_books = calculate_mean_ratings(rat)






    user_book = json.loads(user['rating'])

    set_book = set()

    list_of_set = []

    for book in user_book:
        responseB = requests.get('http://localhost:9200/goodreads_item/_doc/' + str(book['book_id']))
        try:
            vector = responseB.json()['_source']['similarity-vector']
        except:
            continue
        res = es.knn_search(index='goodreads_item',
                            knn={"field": "similarity-vector", "query_vector": vector, "k": 500, "num_candidates": 1000})
        for hit in res['hits']['hits']:
            if hit['_id'] != book['book_id']:
                set_book.add(int(hit['_id']))
        list_of_set.append(set_book)

    # combine all set
    try :
        set_book = set.union(*list_of_set)
    except:
        return []
    inter = set.intersection(set_book, unread_books)

    return inter


def evaluate_user_based():
# read testset and get user_id
    testset = pd.read_csv('test_set.csv')
    user_id = testset['user_id'].unique()
    '''
    res= find_similar_books(0)
    print(res)
    # sort by rating
    res  = dict(sorted(res.items(), key=lambda item: item[1], reverse=True))
    print(res)
    '''
    res=[]

    for user in user_id:
        books = find_similar_books(user)
        try :
            book_id_test = testset[testset['user_id'] == user]['book_id'].unique()[0]

            if book_id_test in books.keys():
                print("user: ", user, "books: ", books, "book_id_test: ", book_id_test)
                res.append(books[book_id_test])
            else:
                res.append(0)
        except:
            res.append(0)
    acc= 5-np.mean(res)
    print("Accuracy: ", acc)





def evaluate_hybrid():
    # read testset and get user_id
    testset = pd.read_csv('test_set.csv')
    user_id = testset['user_id'].unique()
    res = []
    books = find_similar_hybrid(1)
    '''
    for user in user_id:
        books = find_similar_hybrid(user)
        book_id_test = testset[testset['user_id'] == user]['book_id'].unique()[0]

        if book_id_test in books:
            print("user: ", user, "book_id_test: ", book_id_test)
            res.append(1)
        else:
            res.append(0)

    print("Accuracy: ", np.mean(res))
    '''

def request(user, method='hybrid'):
    if method == 'user_based':
        res = find_similar_books(user)
    else:
        res = find_similar_hybrid(user)
    return res

if __name__ == '__main__':
    #evaluate_user_based()
    #evaluate_hybrid()
    res = request(1, 'user_based')
    print(res)
    # find name of book
    df = pd.read_csv('book_id_map.csv')

    # get book_id

    book_id = []

    for book in res:
        #get book_id from book_id_map
        book_id.append(df[df['book_id_csv'] == book]['book_id'].unique()[0])

    print(book_id)



