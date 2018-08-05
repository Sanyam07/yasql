import re
from collections import OrderedDict

import sqlparse
from funcy import print_calls
from mako.template import Template
from sqlalchemy import text, table
from sqlalchemy.sql.expression import TextAsFrom

def sql_format(sql):
    sql = sql.strip()
    if not sql.endswith(';'):
        sql = sql + ';'
    return sqlparse.format(sql.strip(), reindent=True, keyword_case='upper')


def text_from(sql):
    # If sql is a single word, it should be a table
    # Otherwise, it should be a sql
    if ' ' in sql:
        return TextAsFrom(text(sql), [])
    else:
        return table(sql)


def listify(item):
    if isinstance(item, list):
        return item
    elif isinstance(item, str):
        return [item]
    elif isinstance(item, OrderedDict):
        return [{k: v} for k, v in item.items()]
    raise Exception("Cannot listfy object: {}".format(item))


def dict_one(item):
    assert len(item) == 1
    return item.copy().popitem()


def merge_vars(*layers, dict_cls=OrderedDict):
    def _merge_dict(d1, d2):
        result = d1.copy()
        for k, v in d2.items():
            if k in result and isinstance(result[k], dict_cls):
                result[k] = _merge_dict(result[k], v)
            else:
                result[k] = v
        return result

    def _merge_two(d1, d2):
        result = dict_cls()
        for k, v in d1.items():
            if k.endswith('*'):
                result[k[:-1]] = v
            else:
                result[k] = v

        for k, v in d2.items():
            merge = k.endswith('*')
            k = k[:-1] if merge else k
            if merge and (k in result):
                if isinstance(result[k], dict_cls):
                    result[k] = _merge_dict(result[k], v)
                elif isinstance(result[k], list):
                    result[k] += v
            else:
                result[k] = v

        return result

    if len(layers) == 2:
        return _merge_two(*layers)
    return merge_vars(_merge_two(*layers[:2]), *layers[2:], dict_cls=dict_cls)


def inject_vars(item, vars):
    def _expand(val):
        val = val.strip()
        match = re.match(r'^\${\s*(\w+)\s*}$', val)
        if match:
            var_name = match.groups()[0]
            return vars.get(var_name)
        return Template(val).render(**vars)

    if isinstance(item, str):
        return _expand(item)
    elif isinstance(item, list):
        return [inject_vars(i, vars) for i in item]
    elif isinstance(item, OrderedDict):
        return OrderedDict(
            [(k, inject_vars(v, vars)) for k, v in item.items()])
    return item

if __name__ == '__main__':
    print(inject_vars(OrderedDict({'fields': ['col1', '${agg_field}']}), {'agg_field': 'count(*)'}))
