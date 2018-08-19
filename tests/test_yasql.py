#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `yasql` package."""

import os

import pytest
import dateparser as dp
from freezegun import freeze_time
from sqlalchemy import create_engine

from yasql.playbook import Playbook
from yasql.utils import sql_format
from yasql.context import ctx
from yasql.syntax import datetime as dt

def data_path(fname):
    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    return os.path.join(data_dir, fname)

def assert_sql_equal(sql1, sql2):
    assert sql_format(sql1) == sql_format(sql2)

def _test_query(playbook_path, query_name):
    playbook = Playbook.load_from_path(data_path(playbook_path))
    query = playbook.get_query(query_name)
    ctx.playbook = playbook
    ground_truth = query.doc.replace('Expected output:', '').strip()
    print(query.render_sql())
    assert query.render_sql() == sql_format(ground_truth), \
        "Wrong SQL output: {}".format(query_name)

@pytest.fixture
def engine():
    return create_engine('sqlite://')

def setup_module(module):
    # When TIMEZONE is not set to UTC, relative dates gives weird result
    # when working with freeze_time. Since we're not using timezone-aware
    # datetime when parsing date string with dateparser. So it should have
    # no impact
    dt.dt_parse_func = lambda x: dp.parse(x, settings={'TIMEZONE': 'UTC'})

### Basic

def test_simple():
    _test_query('basic.yaml', 'test_simple')

def test_join():
    _test_query('basic.yaml', 'test_join')

def test_vars():
    _test_query('basic.yaml', 'test_vars')

def test_template():
    _test_query('basic.yaml', 'test_template')

def test_with():
    _test_query('basic.yaml', 'test_with')

### Import

def test_import_vars():
    _test_query('import.yaml', 'test_import_vars')

def test_import_templates():
    _test_query('import.yaml', 'test_import_templates')


### Where clause

def test_where_in():
    _test_query('where_clause.yaml', 'test_in')

def test_where_expr():
    _test_query('where_clause.yaml', 'test_sql_expr')

def test_where_or():
    _test_query('where_clause.yaml', 'test_or')

def test_where_abs_date():
    _test_query('where_clause.yaml', 'test_abs_date')

@freeze_time('2018-08-15 10:00:00')
def test_where_rel_date1():
    _test_query('where_clause.yaml', 'test_rel_date1')

@freeze_time('2018-08-15 10:00:00')
def test_where_rel_date2():
    _test_query('where_clause.yaml', 'test_rel_date2')
