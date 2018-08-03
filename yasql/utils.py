from collections import OrderedDict
import sqlparse

def sql_format(sql):
    sql = sql.strip()
    if not sql.endswith(';'):
        sql = sql + ';'
    return sqlparse.format(sql.strip(), reindent=True, keyword_case='upper')

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
