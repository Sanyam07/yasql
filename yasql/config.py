import os

import yaml
import pytz
from dateutil.tz import tzlocal

default_path = os.path.join(os.environ['HOME'], '.yasqlrc')
class Config(object):
    def __init__(self, path=default_path):
        if os.path.exists(path):
            self.data = yaml.load(open(path))
        else:
            self.data = {}

    def update(self, conf):
        self.data.update(conf)

    @property
    def timezone(self):
        tz = self.data.get('timezone')
        return pytz.timezone(tz) if tz else tzlocal()

cfg = Config()
