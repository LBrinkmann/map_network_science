import jsonlines
import requests
import random
import time
import json
import logging

log = logging.getLogger(__name__)

# io


def load_json(filename):
    with open(filename) as f:
        return json.load(f)


def write_json_lines(stream, filename):
    with jsonlines.open(filename, mode='w', flush=True) as writer:
        for obj in stream:
            writer.write(obj)


def load_json_lines(filename):
    with jsonlines.open(filename) as reader:
        for obj in reader:
            yield obj


def delay(iterable, delay):
    for i in iterable:
        yield i
        sec = (delay * random.random())**2 + delay * random.random()
        log.info(f'Sleep: {sec}')
        time.sleep(int(sec))


def get_json(url, headers, url_params, **params):
    resp = requests.get(url=url.format(**url_params), headers=headers, params=params)
    return resp.json()


def post_json(url, headers, method, body, **params):
    resp = requests.post(url=url.format(method=method), json=body, headers=headers, params=params)
    return resp.json()['Results']


def log_stream(stream, name):
    for i, st in enumerate(stream):
        log.debug(f"{name}#{i}: {st}")
        yield st


def key_to_pos(func, *kwargs):
    def _func(*args):
        return func(**{k: v for k,v in zip(kwargs, args)})
    return _func
