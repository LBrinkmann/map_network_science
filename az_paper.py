import datetime
import requests
import time
import re
from functools import partial
from tools import load_json_lines, write_json_lines

ms_academic_base = "https://api.labs.cognitive.microsoft.com/academic/v1.0/{method}?"
input_filename = "data/gs_titles_180516_1234.jsonl"
#input_filename = "data/test.jsonl"

output_filename_pattern = 'data/ms_paper_{date}.jsonl'
headers = {"Ocp-Apim-Subscription-Key": "7c75467b9d30409cbc1a0b77ad565454"}

def get_ms_academic(url, headers, url_params, **params):
    time.sleep(1)
    resp = requests.get(url=url.format(**url_params), headers=headers, params=params)
    return resp.json()

get_ms_ac_paper = partial(
    get_ms_academic, url=ms_academic_base, url_params={'method': 'evaluate'}, attributes='*',
    headers=headers)

def add_ms_ac_info():
    filename = output_filename_pattern.format(date=datetime.datetime.now().strftime("%y%m%d_%H%M"))
    papers = load_json_lines(input_filename)
    papers = (
        {'ms_academic': get_ms_ac_paper(expr="and(Ti='" + re.sub(" $", "", re.sub("[\:\'\,\?\!\.\-] ?", " ", p['title'].lower())) + "',Y>=2014)"), **p}
        for p in papers
    )
    write_json_lines(papers, filename)

add_ms_ac_info()

# {
#   "path": "/paper1/CitationIDs/paper2/AuthorIDs/author",
#   "paper1": {
#     "type": "Paper",
#     "id": [2103852640],
#     "select": ["PublishYear", "OriginalTitle"],
#   },
#   "paper2": {
#     "type": "Paper",
#     "select": [ "*" ],
#   },
#   "author": {
#   	"type": "Author",
#     "select": [ "*" ]
#   }
# }
