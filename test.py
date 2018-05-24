import datetime
import requests
import time
from functools import partial
from tools import load_json_lines, write_json_lines

ms_academic_base = "https://api.labs.cognitive.microsoft.com/academic/v1.0/{method}?"
input_filename = "data/ms_paper_180516_1921.jsonl"
# output_filename_pattern = 'data/ms_paper_{date}.jsonl'
# headers = {"Ocp-Apim-Subscription-Key": "7c75467b9d30409cbc1a0b77ad565454"}

def test():
    papers = load_json_lines(input_filename)
    for p in papers:
        print(p['title'], (p['ms_academic']['expr']), len(p['ms_academic']['entities']) if 'expr' in p['ms_academic'] else 'None')

test()
