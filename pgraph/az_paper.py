import docopt
import re
import os
from functools import partial
from tools import load_json_lines, write_json_lines, delay, get_json, log_stream
import logging

log = logging.getLogger(__name__)

ms_academic_base = "https://api.labs.cognitive.microsoft.com/academic/v1.0/{method}?"
headers = {"Ocp-Apim-Subscription-Key": os.environ['MS_ACADEMIC_KEY']}


get_mc_ac_paper = partial(
    get_json, url=ms_academic_base, url_params={'method': 'evaluate'}, attributes='Ti',
    headers=headers)


normalize_title = (
        lambda title: re.sub(" $", "", re.sub("[\:\'\,\?\!\.\-] ?", " ", title.lower()))
    )


def add_ms_ac_info(input_file, output_file):
    papers = load_json_lines(input_file)
    papers = log_stream(papers, name='Input')
    papers_parsed = (
        {'ms_academic': 
            get_mc_ac_paper(expr="and(Ti='" + normalize_title(p['title']) + "',Y>=2014)"), **p}
        for p in delay(papers, 2)
    )
    papers_parsed_printed = log_stream(papers_parsed, name='Output')
    write_json_lines(papers_parsed_printed, output_file)


### main ###

def main():
    '''
Get papers from Mircosoft Academic

Usage:
    az_paper.py <input_file> <output_file>
    '''

    args = docopt.docopt(main.__doc__)
    args_clean = {arg[1:-1]: val for arg, val in args.items()}
    add_ms_ac_info(**args_clean)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO, format='[%(asctime)s] %(levelname)s@%(module)s.%(funcName)s>> %(message)s')
    main()
