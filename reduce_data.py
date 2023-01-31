import pandas as pd
from random import *

seed(0)

ds = pd.read_csv('ratings.csv')

# drop book if book_id is superior to a certain value
# to keep less book (1%)
ds = ds.drop(ds[ds['book_id'] > 25000].index)

ds = ds.drop(ds.sample(frac=.95).index)

"""
random_ids = [randint(ds['book_id'].min(), ds['book_id'].max()) for i in range(450000)]
ds = ds.drop(ds[ds['book_id'].isin(random_ids)].index)
"""

nbUser = ds['user_id'].unique().shape[0]
nbBook = ds['book_id'].unique().shape[0]

print("nombre d'users : " + str(nbUser))
print("nombre de livres : " + str(nbBook))
print("taux de données manquantes : ", 100 - (ds.shape[0] / (nbUser * nbBook)) * 100, "%")

ds.to_csv('goodreads_interactions_reduced.csv', index=False)