import json
import logging
import os
import sys

import requests

from newrelic import NewRelic
from http_session import new_retry_session

logger = logging.getLogger('nri-databricks')

executor_keys = {'id': 'id', 'hostPort': 'hostPort', 'isActive': 'isActive', 'rddBlocks': 'rddBlocks',
                 'memoryUsed': 'memoryUsed', 'diskUsed': 'diskUsed', 'totalCores': 'totalCores',
                 'maxTasks': 'maxTasks', 'activeTasks': 'activeTasks', 'failedTasks': 'failedTasks',
                 'completedTasks': 'completedTasks', 'totalTasks': 'totalTasks', 'totalDuration': 'totalDuration',
                 'totalGCTime': 'totalGCTime', 'totalInputBytes': 'totalInputBytes',
                 'totalShuffleRead': 'totalShuffleRead', 'totalShuffleWrite': 'totalShuffleWrite',
                 'isBlacklisted': 'isBlacklisted', 'maxMemory': 'maxMemory', 'addTime': 'addTime'}

job_keys = {'jobId': 'jobId', 'name': 'name', 'submissionTime': 'submissionTime', 'jobGroup': 'jobGroup',
            'status': 'status', 'numTasks': 'numTasks', 'numActiveTasks': 'numActiveTasks',
            'numCompletedTasks': 'numCompletedTasks', 'numSkippedTasks': 'numSkippedTasks',
            'numFailedTasks': 'numFailedTasks', 'numKilledTasks': 'numKilledTasks',
            'numCompletedIndices': 'numCompletedIndices', 'numActiveStages': 'numActiveStages',
            'numCompletedStages': 'numCompletedStages', 'numSkippedStages': 'numSkippedStages',
            'numFailedStages': 'numFailedStages'}

stage_keys = {'stageId': 'stageId', 'name': 'name', 'status': 'status', 'attemptId': 'attemptId',
              'numTasks': 'numTasks', 'schedulingPool': 'schedulingPool',
              'numActiveTasks': 'numActiveTasks', 'numCompleteTasks': 'numCompleteTasks',
              'numFailedTasks': 'numFailedTasks', 'numKilledTasks': 'numKilledTasks',
              'numCompletedIndices': 'numCompletedIndices', 'executorRunTime': 'executorRunTime',
              'executorCpuTime': 'executorCpuTime', 'submissionTime': 'submissionTime',
              'firstTaskLaunchedTime': 'firstTaskLaunchedTime', 'inputBytes': 'inputBytes',
              'inputRecords': 'inputRecords', 'outputBytes': 'outputBytes', 'outputRecords': 'outputRecords',
              'shuffleReadBytes': 'shuffleReadBytes', 'shuffleReadRecords': 'shuffleReadRecords',
              'shuffleWriteBytes': 'shuffleWriteBytes', 'shuffleWriteRecords': 'shuffleWriteRecords',
              'memoryBytesSpilled': 'memoryBytesSpilled', 'diskBytesSpilled': 'diskBytesSpilled'}

stream_stat_keys = {'batchDuration': 'batchDuration', 'numReceivers': 'numReceivers',
                    'numActiveReceivers': 'numActiveReceivers', 'numInactiveReceivers': 'numInactiveReceivers',
                    'numTotalCompletedBatches': 'numTotalCompletedBatches',
                    'numRetainedCompletedBatches': 'numRetainedCompletedBatches',
                    'numActiveBatches': 'numActiveBatches', 'numProcessedRecords': 'numProcessedRecords',
                    'numReceivedRecords': 'numReceivedRecords', 'avgInputRate': 'avgInputRate',
                    'avgSchedulingDelay': 'avgSchedulingDelay', 'avgProcessingTime': 'avgProcessingTime',
                    'avgTotalDelay': 'avgTotalDelay'}


def execute_spark_request(url):
    try:
        response = requests.get(url)
        if response.status_code != 200:
            error_message = f'spark ui request failed. ' \
                            f'url:{url}, ' \
                            f'status-code:{response.status_code}, ' \
                            f'reason: {response.reason} ' \
                            f'response: {response.text} '
            logger.error(error_message)
            return None
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.exception(f"error executing spark request to url {url}")


# def post_metrics(nr_metrics):
#     nr_session = new_retry_session()
#     max_metrics = 2000
#     metric_batches = [nr_metrics[i:i + max_metrics] for i in range(0, len(nr_metrics), max_metrics)]
#
#     for metric_batch in metric_batches:
#         status_code = NewRelic.post_events(nr_session, metric_batch)
#         if status_code != 200:
#             logger.error(f'newrelic metric collector responded with status code {status_code}')
#         else:
#             logger.info(f'{len(metric_batch)} metrics sent to newrelic metric collector')


class Integration:

    def __init__(self, config):
        if 'spark' in config:
            spark_config = config['spark']
        else:
            logger.error(f'config file is missing "spark" section')
            sys.exit(f'config file is missing "spark" section')

        if 'newrelic' in config:
            newrelic_config = config['newrelic']
        else:
            logger.error(f'config file is missing "newrelic" section')
            sys.exit(f'config file is missing "newrelic" section')

        self.labels = config['labels']

        if os.environ['NEWRELIC_TAGS']:
            tags = os.environ['NEWRELIC_TAGS']
            try:
                tags_json = json.loads(tags)
                for k, v in tags_json.items():
                    self.labels[k] = v
            except ValueError as e:
                logger.error(f'Ignoring NEWRELIC_TAGS as its value is not valid json', exc_info=True)

        self.driver_host = spark_config['driver_host']
        self.spark_conf_ui_port = spark_config['conf_ui_port']
        self.spark_master_ui_port = spark_config['master_ui_port']
        cluster_name = spark_config['cluster_name']

        self.labels['driverHost'] = self.driver_host
        self.labels['clusterName'] = cluster_name

        newrelic_account_id = newrelic_config['account_id']
        newrelic_api_endpoint = newrelic_config['api_endpoint']

        NewRelic.events_api_key = newrelic_config['api_key']
        NewRelic.set_api_endpoint(newrelic_api_endpoint, newrelic_account_id)

    def run(self):
        logger.debug("Executing integration")

        if self.spark_master_ui_port == '<<MASTER_UI_PORT>>':
            try:
                with open('/tmp/master-params', mode='rt', encoding='utf-8') as f:
                    data = f.read()
                    tokens = data.split(' ')
                    if len(tokens) > 1:
                        logger.info(f"setting spark master_ui_port = {tokens[1]}")
                    self.spark_master_ui_port = tokens[1]
            except OSError:
                logger.error('error opening /tmp/master-params file', exc_info=True)
            except IndexError:
                logger.error('error reading /tmp/master-params file', exc_info=True)

        if self.driver_host == '<<CONF_PUBLIC_DNS>>' or self.spark_conf_ui_port == '<<CONF_UI_PORT>>':
            with open('/tmp/driver-env.sh', mode='rt', encoding='utf-8') as f:
                lines = f.readlines()
                for line in lines:
                    tokens = line.split("=")
                    if len(tokens) > 1:
                        if tokens[0].strip() == 'CONF_PUBLIC_DNS':
                            self.driver_host = tokens[1].strip()
                            logger.info(f"extracting driver_host = {self.driver_host}")
                        elif tokens[0].strip() == 'CONF_UI_PORT':
                            self.spark_conf_ui_port = tokens[1].strip()
                            logger.info(f"extracting conf_public_dns = {self.spark_conf_ui_port}")

        master_json_url = f'http://{self.driver_host}:{self.spark_master_ui_port}/json/'
        master_json = execute_spark_request(master_json_url)
        if master_json:
            for active_app in master_json['activeapps']:
                print(active_app['id'])
                self.get_jobs_for_app(active_app['id'])
                self.get_stages_for_app(active_app['id'])
                self.get_executors_for_app(active_app['id'])
                self.get_statistics_for_app(active_app['id'])

    def get_jobs_for_app(self, app_id):
        nr_events = []
        url = f'http://{self.driver_host}:{self.spark_conf_ui_port}/api/v1/applications/{app_id}/jobs'
        jobs_json = execute_spark_request(url)
        logger.debug("Processing jobs")
        for job in jobs_json:
            logger.debug(job)
            nr_event = {key: value for key, value in job.items() if key in job_keys}
            nr_event['eventType'] = 'SparkJob'
            nr_event.update(self.labels)
            nr_events.append(nr_event)
        if nr_events:
            self.post_events(nr_events)

    def get_stages_for_app(self, app_id):
        nr_events = []
        url = f'http://{self.driver_host}:{self.spark_conf_ui_port}/api/v1/applications/{app_id}/jobs'
        stages_json = execute_spark_request(url)
        logger.debug("Processing stages")
        for stage in stages_json:
            logger.debug(stage)
            nr_event = {key: value for key, value in stage.items() if key in stage_keys}
            nr_event['eventType'] = 'SparkStage'
            nr_event.update(self.labels)
            nr_events.append(nr_event)
        if nr_events:
            self.post_events(nr_events)

    def get_executors_for_app(self, app_id):
        nr_events = []
        url = f'http://{self.driver_host}:{self.spark_conf_ui_port}/api/v1/applications/{app_id}/executors'
        executors_json = execute_spark_request(url)
        logger.debug("Processing executors")
        for executor in executors_json:
            logger.debug(executor)
            nr_event = {key: value for key, value in executor.items() if key in executor_keys}
            nr_event['eventType'] = 'SparkExecutor'
            nr_event.update(self.labels)
            for k, v in executor['memoryMetrics'].items():
                nr_event[k] = v
            nr_events.append(nr_event)
        if nr_events:
            self.post_events(nr_events)

    def get_stages_for_app(self, app_id):
        nr_events = []
        url = f'http://{self.driver_host}:{self.spark_conf_ui_port}/api/v1/applications/{app_id}/streaming/statistics'
        stream_stats_json = execute_spark_request(url)
        logger.debug("Processing streaming statistics")
        for stream_stats in stream_stats_json:
            logger.debug(stream_stats)
            nr_event = {key: value for key, value in stream_stats.items() if key in stream_stat_keys}
            nr_event['eventType'] = 'SparkStreamingStatistics'
            nr_event.update(self.labels)
            nr_events.append(nr_event)
        if nr_events:
            self.post_events(nr_events)

    def post_events(self, nr_events):
        nr_session = new_retry_session()
        # since the max number of events that can be posted in a single payload to New Relic is 2000
        max_events = 2000
        events_batches = [nr_events[i:i + max_events] for i in range(0, len(nr_events), max_events)]

        for events_batch in events_batches:
            status_code = NewRelic.post_events(nr_session, events_batch, self.labels)
            if status_code != 200:
                logger.error(f'newrelic events collector responded with status code {status_code}')
            else:
                logger.info(f"{len(events_batch)} events posted to newrelic event collector")
