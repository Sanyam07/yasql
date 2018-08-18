import os
import re
import copy
from collections import Counter

from funcy import cached_property, merge

from .base import dict_cls
from .config import cfg
from .yaml_parser import load
from .sql_render import SQLRender
from .utils import sql_format, overrides, inject_vars, listify, dict_one


class DuplicateQueryNames(Exception):
    pass

class QueryNotExists(Exception):
    pass


def query_select_with(query, data):
    def _process_one(item):
        if isinstance(item, str):
            alias = item
            sql = playbook.get_query(item).render_sql(engine)
            return dict_cls({item: sql})
        elif isinstance(item, dict_cls):
            alias, subquery = dict_one(item)
            sql = playbook.get_query(subquery).render_sql(engine)
        else:
            raise Exception("Invalid format in with: {}".format(item))
        return dict_cls({alias: re.sub(';$', '', sql)})

    playbook = query.playbook
    engine = query.engine
    select_with = data.get_path('select.with')
    if not select_with:
        return data

    select_with = listify(select_with)
    select_with = [_process_one(item) for item in select_with]
    data['select']['with'] = select_with

    return data


def query_vars(query, data):
    playbook_vars = query.playbook.get('vars', {})
    query_vars = data.get('vars', {})
    vars = overrides(playbook_vars, query_vars)
    if vars:
        data = inject_vars(data, vars)
    return data


def query_template(query, data):
    if 'template' not in data:
        return data

    templates = query.playbook.get('templates', {})
    tmpl_path = data.pop('template')
    tmpl = templates.get_path(tmpl_path)
    if not tmpl:
        raise Exception('Template not found: {}'.format(tmpl_path))
    return merge(data, tmpl)


class Query(object):
    keywords = [
        query_template,
        query_select_with,
        query_vars
    ]

    def __init__(self, data, playbook):
        self.name = data.get('name')
        self.playbook = playbook
        self.data = data
        self.engine = None

    def process_keywords(self, data):
        for kw in self.keywords:
            data = kw(self, data)
        return data

    def render_sql(self, engine):
        self.engine = engine
        data = copy.deepcopy(self.data)
        data = self.process_keywords(data)
        query = SQLRender(data).render()
        query = str(query.compile(engine,
                                  compile_kwargs={"literal_binds": True}))
        return sql_format(query)

    @property
    def doc(self):
        return self.data.get('doc')

class Playbook(object):
    def __init__(self, content, path=None):
        data = load(content)
        self.path = path
        self.data = self.process_imports(data)
        self.update_config()

    @classmethod
    def load_from_path(cls, path):
        with open(path) as f:
            return Playbook(f.read(), path)

    def update_config(self):
        cfg.update(self.data.get('config', {}))

    def process_imports(self, data):
        imports = data.get('imports', [])
        base_dir = os.path.dirname(self.path)
        for imp in imports:
            playbook = self.load_from_path(os.path.join(base_dir, imp['from']))
            namespace = imp.get('as')
            keys = listify(imp['import'])
            for key in keys:
                imported = playbook.get(key)
                if not imported:
                    raise Exception("{} doesn't exist in {}".format(
                        key, imp['from']))
                data.setdefault(key, dict_cls())
                if namespace:
                    data[key].setdefault(namespace, dict_cls()).update(imported)
                else:
                    data[key] = overrides(imported, data[key])
        return data

    def get(self, key, default=None):
        return self.data.get(key, default)

    def get_query(self, query_name):
        queries = [q for q in self.queries if q.name == query_name]
        if queries:
            return queries[0]
        else:
            raise QueryNotExists(query_name)

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
