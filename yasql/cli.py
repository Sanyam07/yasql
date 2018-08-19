# -*- coding: utf-8 -*-

"""Console script for yasql."""
import sys
import logging

import click

from .context import ctx
from .playbook import Playbook

def setup_logger(level):
    logging.basicConfig(
        stream=sys.stdout,
        level=level,
        format='[%(asctime)s] [%(levelname)s] %(message)s'
    )
    if level == logging.DEBUG:
        # echo all queries sent to DB
        logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

@click.command()
@click.argument('playbook')
@click.option('-v', '--verbose', is_flag=True,
              help='Print verbose running stats')
@click.option('--quiet', is_flag=True,
              help='Suppress running stats')
@click.option('--dry', is_flag=True,
              help='Print out execution plan')
@click.option('-q', '--query',
              help='Only execute specified queries (separated by comma)')
@click.option('-p', '--print', is_flag=True,
              help='Print out query results')
@click.option('-m', '--max-rows', type=int, default=100,
              help='Print out query results')
def cli(playbook, **kwargs):
    """ Command-line Tool for executing Yasql """
    if kwargs.get('verbose'):
        log_level = logging.DEBUG
    elif kwargs.get('quiet'):
        log_level = logging.WARN
    else:
        log_level = logging.INFO
    setup_logger(log_level)
    playbook = Playbook.load_from_path(playbook)
    ctx.playbook = playbook
    ctx.dry = kwargs.get('dry')
    ctx.print_result = kwargs.get('print')
    ctx.config.update({'print_max_rows': kwargs['max_rows']})
    queries = kwargs['query'].split(',') if kwargs['query'] else None
    playbook.execute(queries=queries)
    return 0

def main():
    sys.exit(cli())

if __name__ == "__main__":
    main()
