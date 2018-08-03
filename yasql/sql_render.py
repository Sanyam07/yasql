import copy

import sqlalchemy as sa
from sqlalchemy import text

from .utils import listify, dict_one

def _build_where(conds):
    def _build_one(cond):
        if isinstance(cond, str):
            return text(cond)
        if isinstance(cond, dict):
            col, val = dict_one(cond)
            return (sa.literal_column(col) == val)

    return sa.and_(*[_build_one(c) for c in listify(conds)])


def _build_fields(fields):
    def _build_one(cond):
        if isinstance(cond, str):
            return text(cond)
        if isinstance(cond, dict):
            col_name, sql = dict_one(cond)
            return sa.literal_column(sql).label(col_name)

    return [_build_one(f) for f in listify(fields)]


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

    def transform(self, item):
        select = item['select']
        fields = select.get('fields', ['*'])
        qry = sa.select(_build_fields(fields))
        qry = qry.select_from(text(select['from']))
        if 'where' in select:
            qry = qry.where(_build_where(select['where']))

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
        return self._render(copy.deepcopy(self.data))

    def _render(self, item):
        for i in range(self.MAX_ITERATION):
            for p in self.processors:
                item, completed = p.process(item)
                if completed:
                    return item
        raise Exception(
            "Cannot parse the item within {} iterations:\n{}".format(
                self.MAX_ITERATION, item))
