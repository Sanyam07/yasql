import re

import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.sql.expression import TextAsFrom, Executable, ClauseElement

from funcy import decorator

from yasql.utils import dict_one, listify
from .datetime import parse_dt_expr, is_dt_expr

@decorator
def clause(call, key):
    query, stmt_obj = call._args
    if isinstance(key, str):
        key = [key]
    data = [stmt_obj.get(k) for k in key]
    call._args = [query, *data]
    if all(d is None for d in data):
        return query
    return call()

@clause(key=('from', 'join', 'with'))
def from_clause(query, data, join, with_):
    return FromClause().build(query, data, join, with_)

class FromClause(object):
    def build(self, query, data, join, with_):
        self.ctes = self._build_cte(with_ or [])
        table = self._build_from(data)
        if join:
            joins = self._build_join(join)
            for right, on in joins:
                table = table.join(right, on)
        return query.select_from(table)

    def _parse_from(self, sql):
        # If sql is a single word, it should be a table or cte
        # Otherwise, it should be a sql
        if ' ' in sql:
            return TextAsFrom(text(sql), [])
        elif sql in self.ctes:
            return self.ctes[sql]
        else:
            return sa.table(sql)

    def _build_cte(self, with_):
        ctes = {}
        for item in with_:
            alias, sql = dict_one(item)
            ctes[alias] = TextAsFrom(text(sql), []).cte(alias)
        return ctes

    def _build_from(self, from_):
        if isinstance(from_, str):
            return self._parse_from(from_)
        elif isinstance(from_, dict):
            alias, from_ = dict_one(from_)
            return self._parse_from(from_).alias(alias)

    def _build_join(self, tables):
        def _parse_join(sql):
            table, _, join = re.split(r'\s+(on|ON)\s+', sql)
            return (self._parse_from(table), text(join))

        def _build_one(table):
            if isinstance(table, str):
                return _parse_join(table)
            elif isinstance(table, dict):
                alias, table = dict_one(table)
                table, join_on = _parse_join(table)
                return (table.alias(alias), join_on)

        return [_build_one(t) for t in listify(tables)]


@clause(key='where')
def where(query, data):
    def _build_one(cond):
        if isinstance(cond, str):
            return text('({})'.format(cond))
        col, val = dict_one(cond)
        if col == 'or_':
            return sa.or_(*[_build_one(c) for c in listify(val)])
        col = sa.literal_column(col)
        if isinstance(val, list):
            return col.in_(val)
        elif isinstance(val, str) and is_dt_expr(val):
            dt = parse_dt_expr(val)
            if not isinstance(dt, tuple):
                return (col == dt)
            start, end = dt
            if start and end:
                return (col >= start) & (col < end)
            elif start:
                return (col >= start)
            elif end:
                return (col < end)

        return (col == val)

    conds = sa.and_(*[_build_one(c) for c in listify(data)])
    return query.where(conds)

@clause(key='group_by')
def group_by(query, data):
    fields = [text(g) for g in listify(data)]
    return query.group_by(*fields)

@clause(key='order_by')
def order_by(query, data):
    return query.order_by(*[text(o) for o in listify(data)])

@clause(key='limit')
def limit(query, data):
    return query.limit(data)

@clause(key='having')
def having(query, data):
    return query.having(text(data))
