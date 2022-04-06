import gzip
import json
import logging

from requests import RequestException

logger = logging.getLogger('nri-databricks')


class NewRelicApiException(Exception):
    pass


class NewRelic:
    US_EVENTS_ENDPOINT = "https://insights-collector.newrelic.com/v1/accounts/{account_id}/events"
    EU_EVENTS_ENDPOINT = "https://insights-collector.eu01.nr-data.net/accounts/{account_id}/events"
    CONTENT_ENCODING = 'gzip'

    events_api_endpoint = US_EVENTS_ENDPOINT
    events_api_key = ''

    @classmethod
    def set_api_endpoint(cls, api_endpoint, nr_account_id):
        if api_endpoint == "US":
            api_endpoint = NewRelic.US_EVENTS_ENDPOINT
        elif api_endpoint == "EU":
            api_endpoint = NewRelic.EU_EVENTS_ENDPOINT
        NewRelic.events_api_endpoint = api_endpoint.format(account_id=nr_account_id)
        logger.info(f'Setting New Relic API endpoint {NewRelic.events_api_endpoint}')

    @classmethod
    def post_events(cls, session, data):
        payload = gzip.compress(json.dumps(data).encode())
        headers = {
            "Api-Key": cls.events_api_key,
            "Content-Encoding": cls.CONTENT_ENCODING,
        }
        try:
            r = session.post(cls.events_api_endpoint, data=payload,
                             headers=headers)
        except RequestException as e:
            raise NewRelicApiException(repr(e)) from e
        return r.status_code
