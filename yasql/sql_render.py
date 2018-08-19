import sqlalchemy as sa
from sqlalchemy import text, table
from sqlalchemy.sql.expression import TextAsFrom

from .utils import listify, dict_one
from .syntax.clause import from_clause, where, group_by, order_by, having, limit


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

    def _build_fields(self, fields):
        def _build_one(cond):
            if isinstance(cond, str):
                return text(cond)
            if isinstance(cond, dict):
                col_name, sql = dict_one(cond)
                return sa.literal_column(sql).label(col_name)

        return [_build_one(f) for f in listify(fields)]

    def build_query(self, qry, select, clauses):
        for func in clauses:
            qry = func(qry, select)
        return qry

    def transform(self, item):
        select = item['select']
        fields = select.get('fields', ['*'])
        qry = sa.select(self._build_fields(fields))
        qry = self.build_query(
            qry, select,
            clauses=[from_clause, where, group_by, order_by, having, limit])

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
