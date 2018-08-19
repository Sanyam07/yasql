import os
import re
import copy
import logging
from collections import Counter
from textwrap import indent

from funcy import cached_property, merge, omit
from tabulate import tabulate

from .base import dict_cls
from .config import Config
from .yaml_parser import load
from .sql_render import SQLRender
from .context import ctx
from .utils import sql_format, overrides, inject_vars, listify, dict_one, \
    execute_sql

logger = logging.getLogger(__name__)

class DuplicateQueryNames(Exception):
    pass

class QueryNotExists(Exception):
    pass

def query_select_with(query, data):
    def _process_one(item):
        if isinstance(item, str):
            alias = item
            sql = playbook.get_query(item).render_sql()
            return dict_cls({item: sql})
        elif isinstance(item, dict_cls):
            alias, subquery = dict_one(item)
            sql = playbook.get_query(subquery).render_sql()
        else:
            raise Exception("Invalid format in with: {}".format(item))
        return dict_cls({alias: re.sub(';$', '', sql)})

    playbook = query.playbook
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

def query_output(query, data):
    out = data.get('output', [])
    if isinstance(out, str):
        out = [{'format': 'table', 'name': out}]
    elif isinstance(out, dict_cls):
        out = [out]
    if ctx.print_result:
        out.insert(0, {'format': 'print'})
    print(out)
    return merge(data, {'output': out})


def output_table(query, name):
    sql = query.render_sql()
    sql = 'CREATE TABLE {} AS \n{}'.format(name, query.render_sql())
    result = execute_sql(query.db_conn, sql)
    logger.info('Table %s was created from query %s', name, query.name)
    return result

def output_print(query):
    sql = query.render_sql()
    cursor = execute_sql(query.db_conn, sql)
    rows = cursor.fetchmany(ctx.config.print_max_rows)
    if len(rows) > 0:
        print(tabulate(rows, headers=rows[0].keys()))
        if cursor.rowcount > len(rows):
            remaining_count = cursor.rowcount - len(rows)
            print('Other {} rows are not displayed.'.format(remaining_count))
        print('Total rows: {}'.format(cursor.rowcount))
    else:
        print('0 rows')


class Query(object):
    keywords = [
        query_template,
        query_select_with,
        query_vars,
        query_output
    ]

    output_formats = {
        'table': output_table,
        'print': output_print
    }

    def __init__(self, data, playbook):
        self.name = data.get('name')
        self.playbook = playbook
        self.data = self.process_keywords(data)

    def process_keywords(self, data):
        for kw in self.keywords:
            data = kw(self, data)
        return data

    def render_sql(self):
        query = SQLRender(self.data).render()
        query = str(query.compile(self.db_conn,
                                  compile_kwargs={"literal_binds": True}))
        return sql_format(query)

    def output(self):
        outs = self.data.get('output')
        for out in outs:
            format = out.get('format')
            kwargs = omit(out, 'format')
            if format not in self.output_formats:
                raise Exception('Output not supported: {}'.format(format))
            self.output_formats.get(format)(self, **kwargs)

    def has_output(self):
        return ctx.print_result or bool(self.data.get('output'))

    def execute(self):
        logger.info('Execute query %s', self.name)
        if not self.has_output():
            logger.info("This queries doesn't have any output, "
                        "No commands will be executed.")
            logger.debug('SQL:\n%s', indent(self.render_sql(), '    '))
        else:
            return self.output()

    @property
    def doc(self):
        return self.data.get('doc')

    @property
    def db_conn(self):
        return self.playbook.config.db_conn

class Playbook(object):
    def __init__(self, content, path=None):
        data = load(content)
        self.path = path
        self.data = self.process_imports(data)

    @classmethod
    def load_from_path(cls, path):
        with open(path) as f:
            return Playbook(f.read(), path)

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

    def execute(self, queries=None):
        logger.info('Execute playbook %s', self.path or '')
        if not queries:
            queries = self.queries
        else:
            queries = [self.get_query(q) for q in queries]
        [q.execute() for q in queries]

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

    @cached_property
    def config(self):
        cfg = Config()
        cfg.update(self.data.get('config', {}))
        return cfg
