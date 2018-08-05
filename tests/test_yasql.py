#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `yasql` package."""

import os

from sqlalchemy import create_engine
import pytest

from yasql.playbook import Playbook
from yasql.utils import sql_format

def data_path(fname):
    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    return os.path.join(data_dir, fname)

def assert_sql_equal(sql1, sql2):
    assert sql_format(sql1) == sql_format(sql2)

def _test_query(engine, playbook_path, query_name):
    with open(data_path(playbook_path)) as f:
        playbook = Playbook(f.read())
    query = playbook.get_query(query_name)
    ground_truth = query.doc.replace('Expected output:', '').strip()
    assert query.render_sql(engine) == sql_format(ground_truth), \
        "Wrong SQL output: {}".format(query_name)

@pytest.fixture
def engine():
    return create_engine('sqlite://')

def test_simple(engine):
    _test_query(engine, 'basic.yaml', 'test_simple')

def test_join(engine):
    _test_query(engine, 'basic.yaml', 'test_join')

def test_vars(engine):
    _test_query(engine, 'basic.yaml', 'test_vars')

def test_template(engine):
    _test_query(engine, 'basic.yaml', 'test_template')
