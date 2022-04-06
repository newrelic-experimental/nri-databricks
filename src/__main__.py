#!/usr/bin/env python
import getopt
import logging
import logging.handlers
import os
import sys

from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.schedulers.background import BlockingScheduler
from pytz import utc
from yaml import Loader, load

from integration import Integration

config_dir = None
argv = sys.argv[1:]
print(f'using program arguments {argv}')
try:
    opts, args = getopt.getopt(argv, 'c:', ['config_dir='])
    for opt, arg in opts:
        if opt in ('-c', '--config_dir'):
            config_dir = arg

except getopt.GetoptError as e:
    sys.exit(f'error parsing command line options: {e}')

if config_dir is None:
    config_dir = os.environ.get('CONFIG_DIR')
    if config_dir is None:
        config_dir = os.getcwd()

config_file = f'{config_dir}/config.yml'

if not os.path.exists(config_file):
    sys.exit(f'config file {config_file} not found')


def main():
    with open(config_file) as stream:
        config = load(stream, Loader=Loader)

    run_as_service = config.get('run_as_service', False)

    logger = logging.getLogger('nri-databricks')
    log_level = config.get('log_level', 'info')
    if log_level == 'info':
        logger.setLevel(logging.INFO)
    elif log_level == 'debug':
        logger.setLevel(logging.DEBUG)
    elif log_level == 'warning':
        logger.setLevel(logging.WARNING)
    elif log_level == 'error':
        logger.setLevel(logging.ERROR)
    elif log_level == 'critical':
        logger.setLevel(logging.CRITICAL)
    else:
        logger.setLevel(logging.INFO)

    # Configure logging to log to a file, making a new file at midnight and keeping the last 3 day's data
    log_file = config.get('log_file', '/tmp/nri-databricks.log')
    # Make a handler that writes to a file, making a new file at midnight and keeping 3 backups
    handler = logging.handlers.TimedRotatingFileHandler(log_file, when="midnight", backupCount=3)
    # Format each log message like this
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
    # Attach the formatter to the handler
    handler.setFormatter(formatter)
    # Attach the handler to the logger
    logger.addHandler(handler)

    if not run_as_service:
        integration = Integration(config)
        integration.run()
    else:
        poll_interval = config.get('poll_interval')
        integration = Integration(config)
        jobstores = {
            'default': MemoryJobStore(),
        }
        executors = {
            'default': ThreadPoolExecutor(20),
        }
        job_defaults = {
            'coalesce': False,
            'max_instances': 3
        }
        scheduler = BlockingScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults, timezone=utc)
        scheduler.add_job(integration.run, trigger='interval', seconds=poll_interval)
        scheduler.start()
        print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))


if __name__ == "__main__":
    main()
