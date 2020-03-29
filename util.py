#!/usr/bin/env python3
from functools import reduce
from typing import Tuple, Set, Dict, List, Iterator, Any, Callable
from itertools import groupby, chain


def get_key(tuple_: Tuple[str, Any]) -> str:
    return tuple_[0]


def get_value(tuple_: Tuple[str, Any]) -> str:
    return tuple_[1]


def union_sets(set_1: Set, set_2: Set) -> Set:
    return set_1 | set_2


def modify_dict(base_dict: Dict[str, Any],
                transform_dict: Dict[str, Callable[[Any], Any]],
                default_fn: Callable = lambda x: x) -> Dict[str, Any]:
    return {
        k: transform_dict.get(k, default_fn)(v)
        for k, v in base_dict.items()
    }


def merge_dict(acc: Dict[str, Any], act: Dict[str, Any],
               reduce_fn: Callable[[List[Any]], Any] = lambda x: x) \
               -> Dict[str, Any]:
    def apply_function(iterator: Iterator[Tuple[Any, Any]]) -> Any:
        values = list(get_value(t) for t in iterator)
        return reduce_fn(values) if len(values) > 1 else values[0]

    return {
        k: apply_function(v)
        for k, v in groupby(
            sorted(chain(acc.items(), act.items()), key=get_key), key=get_key)
    }


def del_keys(base_dict: Dict[str, Any],
             key_to_del: Set[str]) -> Dict[str, Any]:
    return {k: v for k, v in base_dict.items() if k not in key_to_del}


if __name__ == '__main__':

    def assert_dicts(generated_dict, expected_dict):
        try:
            assert generated_dict == expected_dict
        except AssertionError:
            print(f'AssertionError {generated_dict} != {expected_dict}')
            raise AssertionError

    def test_modify_default_simple_str_value():
        expected = {'k1': 'mv1', 'k2': 2, 'k3': 'mv3'}
        d = {'k1': 'v1', 'k2': 2, 'k3': 'v3'}
        m = {'k2': lambda x: x}
        dr = modify_dict(d, m, lambda x: 'm' + x)
        assert_dicts(dr, expected)

    def test_modify_simple_int_value():
        expected = {'k1': 'v1', 'k2': 3, 'k3': 'v3'}
        d = {'k1': 'v1', 'k2': 2, 'k3': 'v3'}
        m = {'k2': lambda value: value + 1}
        dr = modify_dict(d, m, lambda x: x)
        assert_dicts(dr, expected)

    def test_modify_simple_str_value():
        expected = {'k1': 'mv1', 'k2': 2, 'k3': 'v3'}
        d = {'k1': 'v1', 'k2': 2, 'k3': 'v3'}
        m = {'k1': lambda value: 'm' + value}
        dr = modify_dict(d, m, lambda x: x)
        assert_dicts(dr, expected)

    def test_modify_map_list_value():
        expected = {'k1': 'v1', 'k2': 2, 'k3': [1, 2, 3]}
        d = {'k1': 'v1', 'k2': 2, 'k3': [0, 1, 2]}
        m = {'k3': lambda value: [v + 1 for v in value]}
        dr = modify_dict(d, m, lambda x: x)
        assert_dicts(dr, expected)

    def test_modify_reduce_list_value():
        from functools import reduce
        expected = {'k1': 'v1', 'k2': 2, 'k3': 3}
        d = {'k1': 'v1', 'k2': 2, 'k3': [0, 1, 2]}
        m = {'k3': lambda value: reduce(lambda acc, act: acc + act, value, 0)}
        dr = modify_dict(d, m, lambda x: x)
        assert_dicts(dr, expected)

    def test_modify_dict_value():
        expected = {
            'k1': 'v1',
            'k2': {
                'k1': 'v1',
                'k2': 3,
                'k3': 'v3'
            },
            'k3': [0, 1, 2]
        }
        d = {
            'k1': 'v1',
            'k2': {
                'k1': 'v1',
                'k2': 2,
                'k3': 'v3'
            },
            'k3': [0, 1, 2]
        }
        m1 = {'k2': lambda value: value + 1}
        m = {'k2': lambda value: modify_dict(value, m1, lambda x: x)}
        dr = modify_dict(d, m, lambda x: x)
        assert_dicts(dr, expected)

    def test_simple_merge():
        expected = {'k1': 'v1', 'k2': 'v2'}
        d1 = {'k1': 'v1'}
        d2 = {'k2': 'v2'}
        dr = merge_dict(d1, d2, {})
        assert_dicts(dr, expected)

    def test_repeated_merge():
        expected = {'k1': 'v1', 'k2': 'v2', 'km': [1, 2]}
        d1 = {'k1': 'v1', 'km': 1}
        d2 = {'k2': 'v2', 'km': 2}
        dr = merge_dict(d1, d2)
        assert_dicts(dr, expected)

    def test_repeated_reduce_merge():
        expected = {'k1': 'v1', 'k2': 'v2', 'km': 3}
        d1 = {'k1': 'v1', 'km': 1}
        d2 = {'k2': 'v2', 'km': 2}
        dr = merge_dict(
            d1, d2, lambda _list: reduce(lambda acc, act: acc + act, _list))
        assert_dicts(dr, expected)

    def test_composed_merge():
        expected = {
            'k1': 'v1',
            'k2': 'v2',
            'km': {
                'k1': 'v1',
                'k2': 'v2',
                'km': 3
            }
        }
        d1 = {'k1': 'v1', 'km': {'k1': 'v1', 'km': 1}}
        d2 = {'k2': 'v2', 'km': {'k2': 'v2', 'km': 2}}
        dr = merge_dict(
            d1, d2, lambda _list: reduce(
                lambda acc, act: merge_dict(
                    acc, act, lambda _int_list: reduce(lambda x, y: x + y,
                                                       _int_list)), _list, {}))
        assert_dicts(dr, expected)

    def test_del_keys_in_dict():
        expected = {'k1': 'v1'}
        d1 = {'k1': 'v1', 'km': {'k1': 'v1', 'km': 1}}
        dr = del_keys(d1, {'km'})
        assert_dicts(dr, expected)

    test_modify_default_simple_str_value()
    test_modify_simple_int_value()
    test_modify_simple_str_value()
    test_modify_map_list_value()
    test_modify_reduce_list_value()
    test_modify_dict_value()
    test_simple_merge()
    test_repeated_merge()
    test_repeated_reduce_merge()
    test_composed_merge()
    test_del_keys_in_dict()
    print('Ok')
