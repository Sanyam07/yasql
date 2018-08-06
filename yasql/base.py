from collections import OrderedDict

class KeyPathError(Exception):
    pass

class DotOrderedDict(OrderedDict):
    def __getattr__(self, attr):
        try:
            return self[attr]
        except KeyError:
            raise AttributeError(attr)

    def get_path(self, path):
        parts = path.split('.')
        result = self
        try:
            for p in parts:
                result = result[p]
            return result
        except KeyError:
            raise KeyPathError(path)

dict_cls = DotOrderedDict
