class Context(object):
    def set_playbook(self, playbook):
        self._playbook = playbook

    @property
    def playbook(self):
        return self._playbook

    @property
    def config(self):
        return self._playbook.config

ctx = Context()
