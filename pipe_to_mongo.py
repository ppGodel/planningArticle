import asyncio
import sys
import time
from functools import partial, reduce
from typing import Set, Dict, List, Any, Callable
import rx
from rx import operators as op
from rx.core import GroupedObservable
from rx.core.observable.connectableobservable import ConnectableObservable
from rx.subjects.subject import Subject
import json
from mongo_conn import mongo_connection, save_one_item, _get_db_to_insert, \
    graph_exists, save_many_items
from util import modify_dict, merge_dict, del_keys, union_sets


def merge_level(acc_level: Dict[str, Any],
                act_level: Dict[str, Any]) -> Dict[str, Any]:
    return modify_dict(merge_dict(acc_level, act_level, lambda x: x),
                       {'level': lambda x: reduce(union_sets, x, {})},
                       lambda x: next(x))


# TODO: change to pure function
def reduce_edge_dict(acc: Dict, act: Dict) -> Dict:
    if 'level' in act.keys():
        if 'level' not in acc.keys():
            acc['level'] = {}
        for typ, levels in act['level'].items():
            if typ not in acc['level'].keys():
                acc['level'][typ] = set()
            acc['level'][typ] = acc['level'][typ] | levels
    return acc


def reduce_node_dict(acc: Dict[str, Set], act: Dict[str,
                                                    Set]) -> Dict[str, Set]:
    return modify_dict(
        acc, {'level': lambda acc_level: acc_level | act.get('level', {})})


def node_dict_to_edge_dict_with_list(act: Dict) -> Dict:
    return modify_dict(
        act,
        {'level': lambda lvls: {k: sorted(list(v))
                                for k, v in lvls.items()}})


def edge_dict_to_node_dict_with_list(act: Dict) -> Dict:
    return modify_dict(act, {'level': lambda v: sorted(list(v))})


def dict_to_dict_with_set(act: Dict) -> Dict:
    return modify_dict(act, {'level': lambda v: set(v)})


def raw_node_dict_to_formatted_node_dict(dic: Dict):
    return merge_dict(dic, {'level': {}}, lambda x: next(x))


# TODO: Cambiar a dict comprehention types and levels
def raw_edge_dict_to_formatted_edge_dict(dic: Dict[str, Any]):
    return merge_dict(
        del_keys(dic, {'level', 'type'}),
        {'level': {
            dic.get('type', ''): set(dict.get('level', []))
        }})


def exists_graph_in_db(dic: Dict[str, Any],
                       db_checker: Callable[[str], bool]) -> bool:
    graph_name = dic.get('graph_name', dic.get('name', ''))
    return db_checker(graph_name)


def check_graph_exists_in_mongo_collection(
        graph_checker: Callable[[str], bool]) -> \
        Callable[[Dict[str, Any]], bool]:
    def _check_graph(graph_dict: Dict[str, Any]) -> bool:
        return exists_graph_in_db(graph_dict, graph_checker)

    return _check_graph


def print_normal(obj: Any):
    print("{}".format(str(obj)))


def dumb(obj: Any):
    return


def get_obj_type_from_type_map(type_map: Dict[str, str], obj_dic: Dict):
    return next(v for k, v in type_map.items() if k in obj_dic.keys())


def subscriber(subscriber_map: Dict, grouped_observable: GroupedObservable):
    subject = subscriber_map.get(grouped_observable.key)
    if subject:
        grouped_observable.subscribe(subject)


def general_edge_grouper(observable: rx.Observable):
    return observable.pipe(
        op.map(raw_edge_dict_to_formatted_edge_dict),
        op.reduce(reduce_edge_dict),
        op.map(edge_dict_to_edge_dict_with_list),
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


pass_path = "~/Documents/planningArticle/resources/my_data.json"

with open(pass_path) as f:
    credentials = json.load(f)
loop = asyncio.get_event_loop()
conn = mongo_connection(credentials, loop)
graph_in_collection_checker = graph_exists(conn)
exists = check_graph_exists_in_mongo_collection(graph_in_collection_checker)

get_db = partial(_get_db_to_insert, conn)
save_graph_in_db = partial(
    save_one_item,
    conn.get_database("planning").get_collection("graphs2"))
save_edges_in_db = partial(
    save_many_items,
    conn.get_database("planning").get_collection("edges2"))
save_nodes_in_db = partial(
    save_many_items,
    conn.get_database("planning").get_collection("nodes2"))

graph_type_map = {"source": "edge", "node_name": "node", "name": "graph"}
get_dict_type = partial(get_obj_type_from_type_map, graph_type_map)

edge_subject, node_subject, graph_subject = Subject(), Subject(), Subject()

processed_edges = edge_subject.pipe(
    op.filter(lambda edge_dic: not exists(edge_dic)),
    op.group_by(lambda dic: "".join(
        [str(v) for k, v in dic.items() if k not in ['level', 'type']])),
    op.map(lambda o: general_edge_grouper(o)), op.merge_all(),
    op.buffer_with_count(1000),
    op.map(lambda dict_list: save_edges_in_db(dict_list)),
    op.buffer_with_count(5), op.map(lambda futures: perform_futures(futures)),
    op.map(lambda results: [r.inserted_ids for r in results])).subscribe(dumb)

processed_nodes = node_subject.pipe(
    op.filter(lambda node_dic: not exists(node_dic)),
    op.group_by(lambda dic: "".join(
        [str(v) for k, v in dic.items() if k not in ['level']])),
    op.map(lambda o: general_node_grouper(o)), op.merge_all(),
    op.buffer_with_count(5000),
    op.map(lambda dict_list: save_nodes_in_db(dict_list)),
    op.buffer_with_count(5), op.map(lambda futures: perform_futures(futures)),
    op.map(lambda results: [r.inserted_ids for r in results])).subscribe(dumb)

graph_subject.pipe(op.filter(lambda graph_dic: not exists(graph_dic)),
                   ).subscribe(save_one_graph_in_db)

subscribe_map = {
    "edge": edge_subject,
    "node": node_subject,
    "graph": graph_subject
}
local_subscriber = partial(subscriber, subscribe_map)

# base_obs = rx.from_(open("streamTest.txt"))
base_obs = rx.from_(sys.stdin)

c = ConnectableObservable(base_obs, Subject())
dict_delimiter_subject = Subject()
ti = time.time()
c.pipe(op.filter(lambda line: '}' in line or '{' in line),
       op.map(lambda line: True)).subscribe(dict_delimiter_subject)

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
