import yaml

from .base import dict_cls

class OrderedLoader(yaml.Loader):
    pass

def construct_mapping(loader, node):
    loader.flatten_mapping(node)
    return dict_cls(loader.construct_pairs(node))

OrderedLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    construct_mapping)

class OrderedDumper(yaml.Dumper):
    pass

def dict_representer(dumper, data):
    return dumper.represent_dict(data.items())

OrderedDumper.add_representer(dict_cls, dict_representer)

def load(content):
    return yaml.load(content, Loader=OrderedLoader)

def dump(obj):
    return yaml.dump(obj, Dumper=OrderedDumper)
