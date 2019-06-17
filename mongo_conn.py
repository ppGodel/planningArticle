from typing import Callable, Dict, Optional
from urllib.parse import quote_plus

import pymongo


def save(get_db_to_insert: Callable[[Dict], pymongo.collection.Collection], json_dict: Dict):
    db_to_insert = get_db_to_insert(json_dict)
    db_to_insert.insert_one(json_dict)


def mongo_connection(credentials: Dict) -> pymongo.MongoClient:
    uri = "mongodb://%s:%s@%s" % (
        quote_plus(credentials["user"]), quote_plus(credentials["password"]), credentials["host"])
    return pymongo.MongoClient(uri)


def _get_db_to_insert(db_conn: pymongo.MongoClient, json_to_insert: Dict) -> \
        Optional[pymongo.collection.Collection]:
    if 'node_name' in json_to_insert:
        collection = db_conn.get_database("planning").get_collection("nodes")
    elif 'source' in json_to_insert:
        collection = db_conn.get_database("planning").get_collection("edges")
    else:
        collection = db_conn.get_database("planning").get_collection("graphs")
    return collection