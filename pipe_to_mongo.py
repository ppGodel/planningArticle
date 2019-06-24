import asyncio
from functools import partial
from typing import Dict, Callable, Tuple

import sys
import rx
from rx import operators as op
from rx.core.observable.connectableobservable import ConnectableObservable
from rx.subjects.subject import Subject
import json

from mongo_conn import mongo_connection, save, _get_db_to_insert, graph_exists

# base_obs = rx.from_(open("streamTest.txt"))
base_obs = rx.from_(sys.stdin)

c = ConnectableObservable(base_obs, Subject())

t = Subject()

c.pipe(
    op.filter(lambda line: '}' in line or '{' in line),
    op.map(lambda line: True)
).subscribe(t)

with open("/users/jhernandez/Documents/planningArticle/resources/my_data.json") as f:
    credentials = json.load(f)
loop = asyncio.get_event_loop()
conn = mongo_connection(credentials, loop)
_graph_exists = graph_exists(conn)
get_db = partial(_get_db_to_insert, conn)
saver = partial(save, get_db)


def reduce_dict_to_dict_with_set(acc: Dict, act: Dict) -> Dict:
    if 'level' in act.keys():
        if 'level' not in acc.keys():
            acc['level'] = {}
        for typ, levels in act['level'].items():
            if typ not in acc['level'].keys():
                acc['level'][typ] = set()
            acc['level'][typ] = acc['level'][typ] | levels
    return acc


def dict_to_dict_with_list(act: Dict) -> Dict:
    xform = {k: v for k, v in act.items()}
    if 'level' in act.keys():
        xform['level'] = {k: sorted(list(v)) for k, v in xform['level'].items()}
    return xform


def dict_to_dict_with_set(act: Dict) -> Dict:
    xform = {k: v for k, v in act.items()}
    if 'level' in act.keys():
        xform['level'] = {xform['level']}
    return xform


def dict_to_formatted_dict(dic: Dict):
    xform = {k: v for k, v in dic.items() if k not in ['level', 'type']}
    if 'level' in dic.keys() and 'type' in dic.keys():
        if 'level' not in xform.keys():
            xform['level'] = {}
        if dic['type'] not in xform['level']:
            xform['level'][dic['type']] = set()
        xform['level'][dic['type']] = dic['level']
    return xform


def map_dict_to_observer(obs: rx.Observable):
    obs.pipe(
        op.map(lambda dic: 'level' in dic),
        op.group_by(lambda dic: "".join([str(v) for k, v in dic.items() if k != 'level'])),

    ).subscribe(lambda obsr: obsr.pipe(
        op.reduce(lambda acc, act: reduce_dict_to_dict_with_set(acc, act)),
        op.map(lambda dic: dict_to_dict_with_list(dic))
    ).subscribe(saver))


def exists(dic: Dict) -> bool:
    value = False
    if 'graph_name' in dic.keys():
        value = _graph_exists(dic['graph_name'])
    elif 'name' in dic.keys():
        value = _graph_exists(dic['name'])
    return value


def saver_async() -> Tuple[Callable, Callable]:
    task = []

    def wrap_saver(dic: Dict):
        task.append(asyncio.ensure_future(saver(dic)))

    def run():
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.gather(*task))
    return wrap_saver, run


nex, com = saver_async()
c.pipe(
    op.buffer(t),
    op.skip(1),
    op.map(lambda lines: "".join(lines).replace('"', '\"')),
    op.filter(lambda line: '}' not in line),
    op.map(lambda line: "{}{}".format(line, "}")),
    op.map(lambda json_str: json.loads(json_str)),
    op.map(lambda dic: dict_to_dict_with_set(dic)),
    # op.take(100),
    op.group_by(lambda dic: 'source' if 'source' in dic.keys() else
    'node_name' if 'node_name' in dic.keys() else 'graph'),
).subscribe(lambda obs:
            obs.pipe(
                op.filter(lambda dic: not exists(dic)),
                op.group_by(lambda dic: "".join(
                    [str(v) for k, v in dic.items() if k not in ['level', 'type']])),

            ).subscribe(lambda obsr:
                        obsr.pipe(
                            op.map(lambda dic: dict_to_formatted_dict(dic)),
                            op.reduce(lambda acc, act: reduce_dict_to_dict_with_set(acc, act)),
                            op.map(lambda dic: dict_to_dict_with_list(dic))
                        ).subscribe(nex)))

c.connect()
com()
