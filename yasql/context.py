class Context(object):
    def __init__(self):
        self.playbook = None
        self.dry = False
        self.print_result = False

    @property
    def config(self):
        return self.playbook.config

ctx = Context()
