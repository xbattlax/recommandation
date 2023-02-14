from surprise import Reader, Dataset, KNNWithMeans
from elasticsearch import Elasticsearch
from sklearn.decomposition import PCA
import numpy as np

# Load the Goodreads interaction dataset
reader = Reader(line_format='user item rating', sep=',', rating_scale=(1, 5), skip_lines=1)
data = Dataset.load_from_file('goodreads_interactions_reduced.csv', reader=reader)

# Train a user-based collaborative filtering model
sim_options = {'user_based': True}
algo = KNNWithMeans(k=5, sim_options=sim_options)
trainset = data.build_full_trainset()
print(trainset)
algo.fit(trainset)

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

# Compute user similarity vectors and reduce their dimensionality with PCA
users = trainset.all_users()
pca = PCA(n_components=512)
reduced_sim = pca.fit_transform(algo.sim)

similarity_vectors = {}
for user in users:
    # Compute the similarity vector for this user
    sim_vector = algo.sim[user]

    #print shape of sim_vector
    reduced_vector = reduced_sim[user]
    # Store the reduced similarity vector in the Elasticsearch index
    doc = {'similarity-vector': reduced_vector.tolist()}
    es.index(index='goodreads_user_qlearning', id=user, body=doc)

# Define the Elasticsearch index mappings for the Q-learning state
index_mapping = {
    "mappings": {
        "properties": {
            "user": {"type": "keyword"},
            "item": {"type": "keyword"},
            "q_value": {"type": "float"}
        }
    }
}

# Define the Elasticsearch index names
q_index_name = 'goodreads_q_learning'

# Create the Elasticsearch index for the Q-learning state
if not es.indices.exists(index=q_index_name):
    es.indices.create(index=q_index_name, body=index_mapping)

# Define function to get the user's similarity vector from Elasticsearch
def get_user_vector(user):
    doc = es.get(index='goodreads_user_qlearning', id=user)
    return np.array(doc['_source']['similarity-vector'])

# Define function to get the Q-value estimate for a user-item pair from Elasticsearch
def get_q_value(user, item):
    doc_id = f"{user}_{item}"
    doc = es.get(index=q_index_name, id=doc_id, ignore=[404])
    return doc['_source']['q_value'] if doc.get('found', False) else 0.0

# Define function to update the Q-value estimate for a user-item pair using Q-Learning
def update_q_value(user, item, reward, alpha=0.1, gamma=0.9, epsilon=0.1):
    # Get the current Q-value estimate for the (user, item) pair from Elasticsearch
    q_value = get_q_value(user, item)

    try :
        items = [i for i in trainset.all_items() if not trainset.ur[user][i]]
        if not items:
            return
        if np.random.uniform() < epsilon:
            # Choose a random item to explore
            next_item = np.random.choice(items)
        else:
            # Choose the item with the highest Q-value estimate
            next_item = max(items, key=lambda i: get_q_value(user, i))
    except:
        return

    # Get the reward for the (user, next_item) pair
    next_sim_vector = get_user_vector(user)
    next_sim_vector_norm = np.linalg.norm(next_sim_vector)
    item_vector_norm = np.linalg.norm(trainset.ir[item].toarray().flatten())
    reward = next_sim_vector @ trainset.ir[next_item].toarray().flatten() / (next_sim_vector_norm * item_vector_norm)

    # Update the Q-value estimate using Q-Learning
    next_q_value = get_q_value(user, next_item)
    updated_q_value = (1 - alpha) * q_value + alpha * (reward + gamma * next_q_value)

    # Store the updated Q-value estimate in Elasticsearch
    doc_id = f"{user}_{item}"
    doc = {'user': user, 'item': item, 'q_value': updated_q_value}
    es.index(index=q_index_name, id=doc_id, body=doc)

@
# Define function to get the top-N recommendations for a user
def get_recommendations(user, n=10):
    # Get the user's similarity vector
    sim_vector = get_user_vector(user)

    # Get the top-N items with the highest Q-value estimate
    items = [i for i in trainset.all_items() if not trainset.ur[user][i]]
    items = sorted(items, key=lambda i: get_q_value(user, i), reverse=True)
    return items[:n]


if __name__ == '__main__':
    # update the Q-value estimate for a user-item pair
    user = 0
    item = 1
    reward = 0.0
    update_q_value(user, item, reward)


    # Get the top-N recommendations for a user
    user = 0
    recommendations = get_recommendations(user)
    print(recommendations)
