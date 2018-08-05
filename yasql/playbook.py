import copy
from collections import Counter

from funcy import cached_property, merge

from .yaml_parser import load
from .sql_render import SQLRender
from .utils import sql_format, merge_vars, inject_vars


class DuplicateQueryNames(Exception):
    pass

class QueryNotExists(Exception):
    pass


def plugin_vars(query, data):
    playbook_vars = query.playbook.get('vars', {})
    query_vars = data.get('vars', {})
    vars = merge_vars(playbook_vars, query_vars)
    if vars:
        data = inject_vars(data, vars)
    return data


def plugin_template(query, data):
    if 'template' not in data:
        return data

    templates = query.playbook.get('templates', {})
    tmpl_name = data.pop('template')
    if tmpl_name not in templates:
        raise Exception('Template not found: {}'.format(tmpl_name))
    return merge(data, templates[tmpl_name])


class Query(object):
    plugins = [
        plugin_template,
        plugin_vars
    ]

    def __init__(self, data, playbook):
        self.name = data.get('name')
        self.playbook = playbook
        self.data = data

    def apply_plugins(self, data):
        for plugin in self.plugins:
            data = plugin(self, data)
        return data

    def render_sql(self, engine):
        data = copy.deepcopy(self.data)
        data = self.apply_plugins(data)
        query = SQLRender(data).render()
        query = str(query.compile(engine,
                                  compile_kwargs={"literal_binds": True}))
        return sql_format(query)

    @property
    def doc(self):
        return self.data.get('doc')

class Playbook(object):
    def __init__(self, content):
        self.data = load(content)

    def get(self, key, default=None):
        return self.data.get(key, default)

    def get_query(self, query_name):
        queries = [q for q in self.queries if q.name == query_name]
        if queries:
            return queries[0]
        else:
            raise QueryNotExists(query_name)

    def render_sql(self, query_name):
        return self.get_query(query_name).render_sql(self.vars)

    @cached_property
    def queries(self):
        queries = [Query(q, self) for q in self.data.get('queries', [])]
        # Check duplicated query names
        names = [q.name for q in queries if q.name]
        names = Counter(names)
        dups = [n for n, cnt in names.items() if cnt > 1]
        if dups:
            raise DuplicateQueryNames(dups)
        return queries
