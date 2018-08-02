from funcy import omit, merge
from ruamel.yaml.comments import CommentedMap

from .utils import sql_format

def _build_clause(name, item):
    key = name.replace(' ', '_')
    if key not in item:
        return ''

    return '{} {}'.format(name, item[key])

def _build_where(item):
    def wrap(v):
        if isinstance(v, str):
            return "'{}'".format(v)
        return v

    if 'where' not in item:
        return ''

    return 'where ' + \
        ' and '.join(
            '{} = {}'.format(k, wrap(v))
            for k, v in item['where'].items())

class Processor(object):
    when = None

    def process(self, item):
        if self.can_apply(item):
            return self.transform(item)
        return item

    def transform(self, item):
        raise NotImplemented()

    def can_apply(self, item):
        if self.when:
            return self.when in item
        return True


class SqlProcessor(Processor):
    when = 'sql'

    def transform(self, item):
        return sql_format(item['sql'])


class SelectProcessor(Processor):
    when = 'select'
    tmpl = """
    select {fields} from {from_}
    {where}
    {group_by}
    {order_by}
    {limit}
    """

    def transform(self, item):
        select = item['select']
        ctx = dict(
            fields=','.join(select['fields']),
            from_=select['from'],
            where=_build_where(select),
            group_by=_build_clause('group by', select),
            order_by=_build_clause('order by', select),
            limit=_build_clause('limit', select))
        sql = self.tmpl.format(**ctx).strip()
        return merge(
            omit(item, ['select']),
            {'sql': sql})

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
                if isinstance(item, str):
                    return item
                if isinstance(item, CommentedMap):
                    item = dict(item)
                item = p.process(item)

        raise Exception(
            "Cannot parse the item within {} iterations:\n{}".format(
                self.MAX_ITERATION, item))
