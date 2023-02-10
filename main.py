# es password: KgSU+VEYRvPvW09c9czx
# Import libraries
from surprise import Reader, Dataset, KNNWithMeans
from elasticsearch import Elasticsearch
import pandas as pd
from sklearn.decomposition import PCA

# Create a reader object and load data
import random

# 228648342 records

# skip randomly 99% of the data
# n = 228648342 #number of records in file
# s = 2286483 #desired sample size
# filename = "data.txt"
# skip = sorted(random.sample(range(n),n-s))

n = 100

# skip row after row 2286483

ds = pd.read_csv('goodreads_interactions_reduced.csv')
# save ds in csv
ds.to_csv('ratings.csv', index=False)
nbUser = ds['user_id'].unique().shape[0]
nbBook = ds['book_id'].unique().shape[0]
print("taux de donnée manquante: ", 100 - (ds.shape[0] / (nbUser * nbBook)) * 100, "%")
# ds = pd.read_csv('goodreads_interactions.csv')

print(ds.shape)
# remove is_read column
ds = ds.drop(columns=['is_read', 'is_reviewed'])

reader = Reader(rating_scale=(1, 5))
data = Dataset.load_from_df(ds, reader)

# remove randomly 1 line per user and keep it in a test set
test_set = []
for user in ds['user_id'].unique():
    user_ds = ds[ds['user_id'] == user]
    # where rating is 5 or 4
    if user_ds[user_ds['rating'] >= 3].shape[0] == 0:
        continue
    else :
        user_ds = user_ds[user_ds['rating'] >= 3]
    test_set.append(user_ds.iloc[random.randint(0, user_ds.shape[0]-1)])
    ds = ds.drop(user_ds.iloc[random.randint(0, user_ds.shape[0]-1)].name)

test_set = pd.DataFrame(test_set)

# save test set in csv
test_set.to_csv('test_set.csv', index=False)
print(test_set.head())


# Build the user-based collaborative filtering model
user_based_cf = KNNWithMeans(k=5, sim_options={'user_based': True})
user_based_cf.fit(data.build_full_trainset())

# save similarity matrix
user_based_cf.compute_similarities()

# save to csv file
df = pd.DataFrame(user_based_cf.sim)
df.to_csv('similarity_matrix.csv', index=False)
print(df.shape)
pca = PCA(n_components=512)
reduced_sim = pca.fit_transform(df)
# Make recommendations for a specific user
#   user_id = 1
#   print("make user rec for user_id: ", user_id)
#   user_predictions = user_based_cf.get_neighbors(user_id, k=5)
#   print(user_predictions)
# Connect to Elasticsearch

print("df",df.memory_usage(index=True).sum() / 1024 ** 2, "MB")
print("ds",ds.memory_usage(index=True).sum() / 1024 ** 2, "MB")
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

# Create a new index with similarity is a dense vector
es.indices.create(index='goodreads_user', body={
    "mappings": {
        "properties": {
            "similarity-vector": {
                "type": "dense_vector",
                "dims": 512,
                "index": True,
                "similarity": "l2_norm"
            },
            "rating": {
                "type": "object",
                "enabled": False

            }
        }
    }
})
# es.indices.create(index='goodreads', ignore=400)

# es.index(index='goodreads', id=1, body={'similarity': df.iloc[1].tolist()})
# Index user based similarity matrix by user_id
for i in range(0, nbUser):
    json_rating_user = ds[ds['user_id'] == i].to_json(orient='records')
    es.index(index='goodreads_user', id=i, body={'similarity-vector': reduced_sim[i].tolist(), 'rating': json_rating_user})
# for i in range(0, nbUser):
#    es.index(index='goodreads', doc_type='user', id=i, body={'similarity': df.iloc[i].tolist()})

# Store the user-similarity matrix as a vector in Elasticsearch
# for user in user_predictions:
#    es.index(index='user_similarity', doc_type='vector', body={'user_id': user_id, 'similar_user_id': user})
#    print('User {} is similar to user {}'.format(user_id, user))
