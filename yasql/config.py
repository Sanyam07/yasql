import os

import yaml
import pytz
from dateutil.tz import tzlocal
from sqlalchemy import create_engine

default_path = os.path.join(os.environ['HOME'], '.yasqlrc')
class Config(object):
    def __init__(self, path=default_path):
        if os.path.exists(path):
            self.data = yaml.load(open(path))
        else:
            self.data = {}
        self.conn_cache = {}

    def update(self, conf):
        self.data.update(conf)

    @property
    def timezone(self):
        tz = self.data.get('timezone')
        return pytz.timezone(tz) if tz else tzlocal()

    @property
    def db_conn(self):
        conn = self.data.get('db_conn', 'sqlite://')
        if not conn in self.conn_cache:
            self.conn_cache[conn] = create_engine(conn)
        return self.conn_cache[conn]
