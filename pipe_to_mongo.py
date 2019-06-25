import asyncio
import sys
import time
from functools import partial
from typing import Dict, List, Any

import rx
from rx import operators as op
from rx.core import GroupedObservable
from rx.core.observable.connectableobservable import ConnectableObservable
from rx.subjects.subject import Subject
import json

from mongo_conn import mongo_connection, save_one_item, _get_db_to_insert, graph_exists, \
    save_many_items


def reduce_edge_dict(acc: Dict, act: Dict) -> Dict:
    if 'level' in act.keys():
        if 'level' not in acc.keys():
            acc['level'] = {}
        for typ, levels in act['level'].items():
            if typ not in acc['level'].keys():
                acc['level'][typ] = set()
            acc['level'][typ] = acc['level'][typ] | levels
    return acc


def reduce_node_dict(acc: Dict, act: Dict) -> Dict:
    if 'level' in act.keys():
        if 'level' not in acc.keys():
            acc['level'] = set()
            acc['level'] = acc['level'] | act['level']
    return acc


def edge_dict_to_edge_dict_with_list(act: Dict) -> Dict:
    xform = {k: v for k, v in act.items()}
    if 'level' in act.keys():
        xform['level'] = {k: sorted(list(v)) for k, v in xform['level'].items()}
    return xform


def node_dict_to_node_dict_with_list(act: Dict) -> Dict:
    xform = {k: v for k, v in act.items()}
    if 'level' in act.keys():
        xform['level'] = sorted(list(xform['level']))
    return xform


def dict_to_dict_with_set(act: Dict) -> Dict:
    xform = {k: v for k, v in act.items()}
    if 'level' in act.keys():
        xform['level'] = {xform['level']}
    return xform


def raw_node_dict_to_formatted_node_dict(dic: Dict):
    xform = {k: v for k, v in dic.items() if k not in ['level']}
    if 'level' in dic.keys():
        if 'level' not in xform.keys():
            xform['level'] = {}
        xform['level'] = dic['level']
    return xform


def raw_edge_dict_to_formatted_edge_dict(dic: Dict):
    xform = {k: v for k, v in dic.items() if k not in ['level', 'type']}
    if 'level' in dic.keys() and 'type' in dic.keys():
        if 'level' not in xform.keys():
            xform['level'] = {}
        if dic['type'] not in xform['level']:
            xform['level'][dic['type']] = set()
        xform['level'][dic['type']] = dic['level']
    return xform


def exists(dic: Dict)-> bool:
    value = False
    if 'graph_name' in dic.keys():
        value = _graph_exists(dic['graph_name'])
    elif 'name' in dic.keys():
        value = _graph_exists(dic['name'])
    return value


def print_normal(obj: Any):
    print("{}".format(str(obj)))


def dumb(obj: Any):
    return


def get_obj_type_from_type_map(type_map: Dict[str, str], obj_dic: Dict):
    return next(v for k, v in type_map.items() if k in obj_dic.keys())


def subscriber(subscriber_map: Dict,  grouped_observable: GroupedObservable):
    subject = subscriber_map.get(grouped_observable.key)
    if subject:
        grouped_observable.subscribe(subject)


def general_edge_grouper( observable: rx.Observable):
    return observable.pipe(
        op.map(lambda dic: raw_edge_dict_to_formatted_edge_dict(dic)),
        op.reduce(lambda acc, act: reduce_edge_dict(acc, act)),
        op.map(lambda dic: edge_dict_to_edge_dict_with_list(dic)),
    )


def general_node_grouper(observable: rx.Observable):
    return observable.pipe(
        op.map(lambda dic: raw_node_dict_to_formatted_node_dict(dic)),
        op.reduce(lambda acc, act: reduce_node_dict(acc, act)),
        op.map(lambda dic: node_dict_to_node_dict_with_list(dic)),
    )


def perform_futures(futures: List):
    pool = asyncio.get_event_loop()
    values = pool.run_until_complete(await_futures(futures))
    return values


async def await_futures(tasks: List):
    return await asyncio.gather(*tasks)


def save_one_graph_in_db(dic: Dict):
    pool = asyncio.get_event_loop()
    pool.run_until_complete(save_graph_in_db(dic))


with open("/users/jhernandez/Documents/planningArticle/resources/my_data.json") as f:
    credentials = json.load(f)
loop = asyncio.get_event_loop()
conn = mongo_connection(credentials, loop)
_graph_exists = graph_exists(conn)
get_db = partial(_get_db_to_insert, conn)
save_graph_in_db = partial(save_one_item, conn.get_database("planning").get_collection("graphs2"))
save_edges_in_db = partial(save_many_items, conn.get_database("planning").get_collection("edges2"))
save_nodes_in_db = partial(save_many_items, conn.get_database("planning").get_collection("nodes2"))

graph_type_map = {"source": "edge", "node_name": "node", "name": "graph"}
get_dict_type = partial(get_obj_type_from_type_map, graph_type_map)

edge_subject, node_subject, graph_subject = Subject(), Subject(), Subject()

processed_edges = edge_subject.pipe(
    op.filter(lambda edge_dic: not exists(edge_dic)),
    op.group_by(lambda dic: "".join(
        [str(v) for k, v in dic.items() if k not in ['level', 'type']])),
    op.map(lambda o: general_edge_grouper(o)),
    op.merge_all(),
    op.buffer_with_count(100),
    op.map(lambda dict_list: save_edges_in_db(dict_list)),
    op.buffer_with_count(5),
    op.map(lambda futures: perform_futures(futures)),
    op.map(lambda results: [r.inserted_ids for r in results])
).subscribe(dumb)

processed_nodes = node_subject.pipe(
    op.filter(lambda node_dic: not exists(node_dic)),
    op.group_by(lambda dic: "".join(
        [str(v) for k, v in dic.items() if k not in ['level']])),
    op.map(lambda o: general_node_grouper(o)),
    op.merge_all(),
    op.buffer_with_count(600),
    op.map(lambda dict_list: save_nodes_in_db(dict_list)),
    op.buffer_with_count(5),
    op.map(lambda futures: perform_futures(futures)),
    op.map(lambda results: [r.inserted_ids for r in results])
).subscribe(dumb)

graph_subject.pipe(
    op.filter(lambda graph_dic: not exists(graph_dic)),
).subscribe(save_one_graph_in_db)

subscribe_map = {"edge": edge_subject, "node": node_subject, "graph": graph_subject}
local_subscriber = partial(subscriber, subscribe_map)


# base_obs = rx.from_(open("streamTest.txt"))
base_obs = rx.from_(sys.stdin)

c = ConnectableObservable(base_obs, Subject())
dict_delimiter_subject = Subject()
ti = time.time()
c.pipe(
    op.filter(lambda line: '}' in line or '{' in line),
    op.map(lambda line: True)
).subscribe(dict_delimiter_subject)

c.pipe(
    op.buffer(dict_delimiter_subject),
    op.skip(1),
    op.map(lambda lines: "".join(lines).replace('"', '\"')),
    op.filter(lambda line: '}' not in line),
    op.map(lambda line: "{}{}".format(line, "}")),
    op.map(lambda json_str: json.loads(json_str)),
    op.map(lambda dic: dict_to_dict_with_set(dic)),
    # op.take(20),
    op.group_by(lambda dic: get_dict_type(dic)),
).subscribe(local_subscriber)


print("Start stream time: {}".format(str(time.time() - ti)))
c.connect()
print("Finish time: {}".format(str(time.time() - ti)))

