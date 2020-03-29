import asyncio
from functools import lru_cache
from typing import Callable, Dict, Optional, List
from urllib.parse import quote_plus

import aiomongo


async def save_one_item(db_to_insert: aiomongo.collection.Collection, json_dict: Dict):
    return await db_to_insert.insert_one(json_dict)


async def save_many_items(db_to_insert: aiomongo.collection.Collection,
                          json_dict_list: List[Dict]):
    return await db_to_insert.insert_many(json_dict_list)


def graph_exists(db_conn: aiomongo.client) -> Callable[[str], bool]:
    async def _check_in_db(graph_name: str):
        db_to_query = db_conn.get_database("planning").get_collection("graphs2")
        return await db_to_query.find_one({'name': graph_name})

    @lru_cache(maxsize=1024)
    def _graph_exists(graph_name: str) -> bool:
        loop = asyncio.get_event_loop()
        res = loop.run_until_complete(_check_in_db(graph_name))
        return res is not None

    return _graph_exists


async def get_client(uri:str, loop: asyncio.AbstractEventLoop):
    client = await aiomongo.create_client(uri=uri, loop=loop)
    return client


def mongo_connection(credentials: Dict, loop: asyncio.AbstractEventLoop) \
        -> aiomongo.client:
    uri = "mongodb://%s:%s@%s" % (
        quote_plus(credentials["user"]), quote_plus(credentials["password"]), credentials["host"])
    return loop.run_until_complete(get_client(uri, loop))


def _get_db_to_insert(db_conn: aiomongo.client, json_to_insert: Dict) -> \
        Optional[aiomongo.collection.Collection]:
    if 'node_name' in json_to_insert:
        collection = db_conn.get_database("planning").get_collection("nodes2")
    elif 'source' in json_to_insert:
        collection = db_conn.get_database("planning").get_collection("edges2")
    else:
        collection = db_conn.get_database("planning").get_collection("graphs2")
    return collection
