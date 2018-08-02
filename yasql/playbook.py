from collections import Counter
from .yaml_parser import YAMLParser
from .sql_render import SQLRender

class DuplicateTaskNames(Exception):
    pass

class TaskNotExists(Exception):
    pass

class Task(object):
    def __init__(self, data):
        self.data = data

    def render_sql(self):
        return SQLRender(self.data).render()

    @property
    def doc(self):
        return self.data.get('doc')

class Playbook(object):
    def __init__(self, content):
        self.doc = YAMLParser(content).doc
        self.vars = self.doc.get('vars', {})
        self.tasks = self.doc.get('tasks', {})
        self._validate_tasks()

    def _validate_tasks(self):
        # Check duplicated task names
        names = [t['name'] for t in self.tasks if t.get('name')]
        names = Counter(names)
        dups = [n for n, cnt in names.items() if cnt > 1]
        if dups:
            raise DuplicateTaskNames(dups)

    def get_task(self, task_name):
        tasks = [t for t in self.tasks if t.get('name') == task_name]
        if tasks:
            return Task(tasks[0])
        else:
            raise TaskNotExists(task_name)
