# based on: https://github.com/danilopolani/rotating-proxy-python


# from urllib.request import Request, urlopen
import requests
from fake_useragent import UserAgent
import logging
import time
from fritz_reconnect import reconnect

log = logging.getLogger(__name__)


ua = UserAgent() # From here we generate a random user agent


def proxy_requests(urls, parser):
    for url in urls:
        print(url)
        for i in range(10):
            try:
                resp = requests.get(url=url, headers={'User-Agent': ua.random})
                if resp.status_code == 200:
                    parsed = parser(resp.content)
                    yield parsed
                    break
                else:
                    reconnect()
                    time.sleep(5)
            except KeyboardInterrupt:
                raise
            except: 
                log.warning(f'Exception with {url}')
                log.warning(resp.content)
        if not parsed:
            print(f'Failed to load {url}.')
