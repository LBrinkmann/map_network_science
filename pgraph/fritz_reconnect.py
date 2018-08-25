# inspired by https://gist.github.com/jmark/db788180a2648ae08031
import requests
import logging

log = logging.getLogger(__name__)


payload = """
<?xml version="1.0" encoding="utf-8"?>
<s:Envelope
s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/" 
xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
<s:Body>
<u:ForceTermination xmlns:u="urn:schemas-upnp-org:service:WANIPConnection:1" />
</s:Body>
</s:Envelope>
"""

url = 'http://fritz.box:49000/igdupnp/control/WANIPConn1'

headers = {
    'Content-Type': 'text/xml; charset=utf-8',
    'SoapAction': 'urn:schemas-upnp-org:service:WANIPConnection:1#ForceTermination'
}


def reconnect():
    log.warning('Reconnect')
    requests.post(url=url, headers=headers, data=payload)