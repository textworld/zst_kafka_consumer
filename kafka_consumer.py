# pip install kafka-python
import json
import logging
import os
import sys
import time
from functools import wraps

import requests
from elasticsearch import Elasticsearch
from kafka import KafkaConsumer
import threading
from apscheduler.schedulers.background import BackgroundScheduler
import itertools

# fail_retry(try_times) 相当于 decorator
# decorator(func) 相当于 wrapper
# wrapper(a) 相当于fun(a)
# @fail_retry(try_times) 相当于 func = fail_retry(try_times)(func)
# 所以 func(a) 相当于 fail_retry(try_times)(func)(a)
def fail_retry(try_times):
    """
    失败重试的装饰器
    :param try_times:
    :return:
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            number = 0
            while number < try_times:
                number += 1
                if number > 1:
                    logging.warning("尝试第%d次", number)
                try:
                    result = func(*args, **kwargs)
                    if result is not None:
                        return result
                    time.sleep(0.1)
                except requests.exceptions.RequestException:
                    time.sleep(0.1)
                    pass
            return None

        return wrapper

    return decorator


@fail_retry(3)
def get_sql_finger_print(soar_host, sql):
    resp = requests.post(soar_host, data=json.dumps({'sql': sql}))
    if resp.status_code >= 200 and resp.status_code < 300:
        return json.loads(resp.text)

    logger.error("got http status code %d: %s", resp.status_code, resp.text)
    return None


def write_elasticsearch(es_host, es_index, slow_log):
    es = Elasticsearch(es_host, http_auth=('elastic', 'password'),)
    es.index(es_index, doc_type="_doc", body=slow_log)


def check_args(arg):
    if arg is None:
        logging.error("arg[%s] is None", arg)
        sys.exit(1)


def get_os_env(name):
    val = os.environ.get(name)
    if val is None:
        raise Exception(f"env[{name}] is None")

    return val


class FastWriteCounter():
    def __init__(self):
        self._number_of_read = 0
        self._counter = itertools.count()
        self._read_lock = threading.Lock()

    def increment(self):
        next(self._counter)

    def value(self):
        with self._read_lock:
            value = next(self._counter) - self._number_of_read
            self._number_of_read += 1
        return value    

def print_counter(counter):
    logging.info('receive %d messages', counter.value())

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger('kafka')
    logger.addHandler(logging.StreamHandler(sys.stdout))
    logger.setLevel(logging.INFO)

    kafka_host = get_os_env('KAFKA_HOST')
    kafka_group_id = get_os_env('KAFKA_GROUP_ID')
    slowlog_topic = get_os_env('SLOW_QUERY_TOPIC')
    es_host = get_os_env('ES_HOST')
    soar_host = get_os_env('SOAR_HOST')
    es_index = get_os_env('ES_INDEX')
    es_user  = get_os_env('ES_USER')
    es_password = get_os_env('ES_PASSWORD')

    consumer = KafkaConsumer(slowlog_topic,
                             bootstrap_servers=[kafka_host],
                             group_id=kafka_group_id,
                             auto_offset_reset='earliest')

    counter = FastWriteCounter()

    scheduler = BackgroundScheduler()
    # 添加任务,时间间隔10S
    scheduler.add_job(print_counter, 'interval', args=(counter,), seconds=10, id='test_job1')

    scheduler.start()

    es = Elasticsearch(es_host, http_auth=(es_user, es_password),)

    for message in consumer:
        counter.increment()
        try:
            val = str(message.value, encoding='utf-8')
            slow_log = json.loads(val)
            slowsql_key = 'slowsql'
            if slowsql_key not in slow_log:
                continue
            data = get_sql_finger_print(soar_host, slow_log[slowsql_key])
            if data is None:
                logging.error('no finger print got')
                sys.exit(1)
            slow_log['finger'] = data['fingerprint']
            slow_log['sql_id'] = data['id']
            'root'.split('_')
            slow_log['schema'] = slow_log['login_user'].split('_')[0]
            es.index(es_index, doc_type="_doc", body=slow_log)
        except json.decoder.JSONDecodeError as jde:
            pass
