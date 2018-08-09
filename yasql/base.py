from collections import OrderedDict

class DotOrderedDict(OrderedDict):
    def __getattr__(self, attr):
        try:
            return self[attr]
        except KeyError:
            raise AttributeError(attr)

    def get_path(self, path, default=None):
        parts = path.split('.')
        result = self
        try:
            for p in parts:
                result = result[p]
            return result
        except KeyError:
            return default

dict_cls = DotOrderedDict
