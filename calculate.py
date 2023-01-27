import pandas as pd


nbUser = pd.read_csv('user_id_map.csv').shape[0]
nbBook = pd.read_csv('book_id_map.csv').shape[0]

tauxRemplissage = 228648342 / (nbUser * nbBook)

print("taux de donnée manquante: ", 100-tauxRemplissage*100, "%")

