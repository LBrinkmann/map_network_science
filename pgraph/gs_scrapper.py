import pandas as pd
import random
import re
import time
import docopt
from itertools import combinations
from bs4 import BeautifulSoup as bs
from tools import write_json_lines
from proxy_drum import proxy_requests
import logging

log = logging.getLogger(__name__)

url_base = 'https://scholar.google.de/scholar?start={start}&hl=en&as_ylo={year}&q={query}'
title_per_page = 10


### tools ### 


def delay(iterable, delay):
    for i in iterable:
        yield i
        sec = (delay * random.random())**2 + delay * random.random() 
        print('Sleep', sec)
        time.sleep(int(sec))

def parser(content):
    soup = bs(content, 'lxml')
    title_h3s = soup.find_all('h3', {'class': 'gs_rt'})
    assert len(title_h3s) > 0
    titles = map(parse_title_h3, title_h3s)
    return titles


def parse_url(query, min_year, start):
    return url_base.format(year=min_year, query=query, start=start)


def parse_title_h3(title_h3):
    return re.match('(\[.*\] )?(?P<title>.*)', title_h3.text)['title']


def create_row(title, idx, query):
    return {'title': title, 'googleScholarInfo': {'idx': idx + query['start'],  **query}}


def find_titles(queries):
    queries = list(queries)
    urls = (parse_url(**query) for query in queries)
    pagetitles = proxy_requests(delay(urls, 5), parser)
    return (   
        create_row(title, idx, query)
        for titles, query in zip(pagetitles, queries)
        for idx, title in enumerate(titles)
    )


### create queries ###


def do_add_quotes(string):
    return '"' + string + '"'


def gen_queries(
        keywords, pages, min_year, title_per_page=title_per_page, add_quotes=True, 
        add_combinations=True):
    """
    Create query object with the fields:
        query: query string
        min_year: starting year for the query
        start: start page
    """
    queries = (
        {'start': st*title_per_page,  'query': kw, 'min_year': min_year}
        for kw in keywords
        for st in range(pages)
    )
    return queries


### entry ###


def get_gs(keyword_file, output_file, min_year=2014, pages=20, scope=''):
    keywords_df = pd.read_csv(keyword_file)
    keywords_df = keywords_df[~keywords_df['include'].isnull()]

    keywords = keywords_df['keyword']
    keywords = '"' + keywords + '" "' + scope + '"'
    keywords = keywords.tolist()

    queries = gen_queries(keywords, int(pages), int(min_year), title_per_page=title_per_page)
    titles = find_titles(queries)
    write_json_lines(titles, output_file)


### main ###


def main():
    '''
Scan Google Scholar

Usage:
    gs_scrapper.py <keyword_file> <output_file> <min_year> <pages> <scope>
    '''

    args = docopt.docopt(main.__doc__)
    args_clean = {arg[1:-1]: val for arg, val in args.items()}
    get_gs(**args_clean)


if __name__ == '__main__':
    main()