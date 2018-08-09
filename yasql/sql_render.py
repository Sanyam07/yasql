import re

import sqlalchemy as sa
from sqlalchemy import text, table
from sqlalchemy.sql.expression import TextAsFrom

from .utils import listify, dict_one


class Processor(object):
    when = None

    def process(self, item):
        if self.can_apply(item):
            return self.transform(item)
        return item, False

    def transform(self, item):
        raise NotImplemented()

    def can_apply(self, item):
        if self.when:
            return isinstance(item, dict) and self.when in item
        return True

class SqlProcessor(Processor):
    when = 'sql'

    def transform(self, item):
        return sa.text(item['sql']), True


class SelectProcessor(Processor):
    when = 'select'

    def __init__(self):
        super().__init__()
        self.ctes = {}

    def _parse_from(self, sql):
        # If sql is a single word, it should be a table or cte
        # Otherwise, it should be a sql
        if ' ' in sql:
            return TextAsFrom(text(sql), [])
        elif sql in self.ctes:
            return self.ctes[sql]
        else:
            return table(sql)

    def _build_cte(self, with_):
        ctes = {}
        for item in with_:
            alias, sql = dict_one(item)
            ctes[alias] = TextAsFrom(text(sql), []).cte(alias)
        return ctes

    def _build_fields(self, fields):
        def _build_one(cond):
            if isinstance(cond, str):
                return text(cond)
            if isinstance(cond, dict):
                col_name, sql = dict_one(cond)
                return sa.literal_column(sql).label(col_name)

        return [_build_one(f) for f in listify(fields)]

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

    def _build_where(self, conds):
        def _build_one(cond):
            if isinstance(cond, str):
                return text(cond)
            if isinstance(cond, dict):
                col, val = dict_one(cond)
                return (sa.literal_column(col) == val)

        return sa.and_(*[_build_one(c) for c in listify(conds)])

    def transform(self, item):
        select = item['select']
        self.ctes = self._build_cte(select.get('with', []))
        fields = select.get('fields', ['*'])
        qry = sa.select(self._build_fields(fields))

        table = self._build_from(select['from'])
        if 'join' in select:
            joins = self._build_join(select['join'])
            for join in joins:
                table = table.join(*join)
        qry = qry.select_from(table)

        if 'where' in select:
            qry = qry.where(self._build_where(select['where']))

        if 'group_by' in select:
            qry = qry.group_by(*[text(g) for g in listify(select['group_by'])])

        if 'order_by' in select:
            qry = qry.order_by(*[text(o) for o in listify(select['order_by'])])

        if 'limit' in select:
            qry = qry.limit(select['limit'])

        return qry, True


class SQLRender(object):
    MAX_ITERATION = 100
    DEFAULT_PROCESSORS = [SqlProcessor, SelectProcessor]

    def __init__(self, data, processors=DEFAULT_PROCESSORS):
        self.data = data
        if not processors:
            processors = self.DEFAULT_PROCESSORS
        self.processors = [cls() for cls in processors]

    def render(self):
        return self._render(self.data)

    def _render(self, item):
        for i in range(self.MAX_ITERATION):
            for p in self.processors:
                item, completed = p.process(item)
                if completed:
                    return item
        raise Exception(
            "Cannot parse the item within {} iterations:\n{}".format(
                self.MAX_ITERATION, item))
