from functools import partial
from typing import Dict

import rx
from rx import operators as op
from rx.core.observable.connectableobservable import ConnectableObservable
from rx.subjects.subject import Subject
import json

from mongo_conn import mongo_connection, save, _get_db_to_insert

base_obs = rx.from_(open("streamTest.txt"))
# base_obs = rx.from_(sys.stdin)

c = ConnectableObservable(base_obs, Subject())

t = Subject()

c.pipe(
    op.filter(lambda line: '}' in line or '{' in line),
    op.map(lambda line: True)
).subscribe(t)

with open("resources/my_data.json") as f:
    credentials = json.load(f)
conn = mongo_connection(credentials)
get_db = partial(_get_db_to_insert, conn)
saver = partial(save, get_db)


def reduce_dict_to_dict_with_set(acc: Dict, act: Dict) -> Dict:
    if 'level' in act.keys():
        acc['level'] = acc['level'] | act['level']
    return acc


def dict_to_dict_with_list(act: Dict) -> Dict:
    xform = {k: v for k, v in act.items()}
    if 'level' in act.keys():
        xform['level'] = list(xform['level'])
    return xform


def dict_to_dict_with_set(act: Dict) -> Dict:
    xform = {k: v for k, v in act.items()}
    if 'level' in act.keys():
        xform['level'] = {xform['level']}
    return xform


c.pipe(
    op.buffer(t),
    op.skip(1),
    op.map(lambda lines: "".join(lines).replace('"', '\"')),
    op.filter(lambda line: '}' not in line),
    op.map(lambda line: "{}{}".format(line, "}")),
    op.map(lambda json_str: json.loads(json_str)),
    op.map(lambda dic: dict_to_dict_with_set(dic)),
    op.group_by(lambda dic: 'source' if 'source' in dic.keys() else
    'node_name' if 'node_name' in dic.keys() else 'graph'),
).subscribe(lambda obs:
            obs.pipe(
                op.filter(lambda dic: 'level' in dic),
                op.group_by(lambda dic: "".join([str(v) for k, v in dic.items() if k != 'level'])),

            ).subscribe(lambda obsr: obsr.pipe(
                op.reduce(lambda acc, act: reduce_dict_to_dict_with_set(acc, act)),
                op.map(lambda dic: dict_to_dict_with_list(dic))
            ).subscribe(saver)))

c.connect()
