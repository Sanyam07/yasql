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

def check_task(engine, playbook, task_name):
    task = playbook.get_task(task_name)
    ground_truth = task.doc.replace('Expected output:', '').strip()
    assert task.render_sql(engine) == sql_format(ground_truth), \
        "Wrong SQL output: {}".format(task_name)

@pytest.fixture
def basic_playbook():
    with open(data_path('basic.yaml')) as f:
        return Playbook(f.read())

@pytest.fixture
def engine():
    return create_engine('sqlite://')

def test_simple(engine, basic_playbook):
    check_task(engine, basic_playbook, 'simple')
