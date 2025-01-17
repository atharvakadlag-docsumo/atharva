from pymongo import MongoClient
import pickle

client_test = MongoClient('mongodb+srv://{}:{}@docsumo-testing.dydiy.mongodb.net/')
client_prod = MongoClient('mongodb+srv://{}:{}@docsumo-testing.dydiy.mongodb.net/')


DB_TO_CLONE_INDEX = ["annotation"] # all if empty
COLLECTION_TO_CLONE_INDEX = ["doc_metadata"] # all if empty

# Function to create multiple indexes
def create_indexes(collection, index_list):
    for index_info in index_list:
        index_keys = index_info.get('key').items()    # Extracting the SON index definition
        del index_info["key"]
        print(index_keys, index_info)
        collection.create_index(index_keys, **index_info)

for db in client_prod.list_database_names():
    if DB_TO_CLONE_INDEX and db not in DB_TO_CLONE_INDEX:
        continue
    for collection in client_prod[db].list_collections():
        if COLLECTION_TO_CLONE_INDEX and collection["name"] not in COLLECTION_TO_CLONE_INDEX:
            continue
    
        collection_test = client_test[db][collection["name"]]
        collection_prod = client_prod[db][collection["name"]]
        try:
            indexes = list(collection_prod.list_indexes())
            with open(f"testing_{db}_{collection['name']}.indexes.dump", "wb") as f:
                pickle.dump(indexes, f)
    
            collection_test.drop_indexes()
            create_indexes(collection_test, indexes)
        except Exception as e:
            print(f"unable to clone indexes for {collection['name']} | {e}")

