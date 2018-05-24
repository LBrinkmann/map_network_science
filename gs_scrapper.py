import pandas as pd
import requests
import datetime
import re
import time
from functools import partial
from itertools import chain, combinations
from bs4 import BeautifulSoup as bs
from tools import load_json_lines, write_json_lines

keywords = [
    'social networks',
    'economic networks',
    'game theory',
    'temporal networks',
    'social capital',
    'collective intelligence',
    'trust networks',
]

url_base = 'https://scholar.google.de/scholar?start={start}&hl=en&as_ylo={year}&q={query}'

min_year = 2014
pages = 1
title_per_page = 10
add_quotes = True
add_combinations = True
output_filename_pattern = 'data/gs_titles_{date}.jsonl'


### tools ### 

def map_delayed(func, iterable, delay):
    for i in iterable:
        yield func(i)
        time.sleep(delay)


### get and parse google scholar results ###

def soup_maker(url):
    r = requests.get(url)
    markup = r.content
    soup = bs(markup, 'lxml')
    return soup

def parse_url(query, min_year, start):
    return url_base.format(year=min_year, query=query, start=0)

def parse_title_h3(title_h3):
    return re.match('(\[.*\] )?(?P<title>.*)', title_h3.text)['title']

def create_row(tup, query):
    idx, title = tup
    return {'title': title, 'googleScholarInfo': {'idx': idx + query['start'],  **query}}

def parse_query(query):
    url = parse_url(**query)
    soup = soup_maker(url)
    title_h3s = soup.find_all('h3', {'class': 'gs_rt'})
    titles = map(parse_title_h3, title_h3s)
    title_rows = map(partial(create_row, query=query), enumerate(titles))
    return title_rows

def find_titles(queries):
    return chain.from_iterable(map_delayed(parse_query, queries, delay=10))


### create queries ###

def add_pages(keyword, pages, title_per_page):
    for st in range(pages):
        yield {'start': st*title_per_page,  'query': keyword}

def add_year(query, min_year):
    return {'min_year': min_year, **query}

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
    if add_quotes:
        keywords = map(do_add_quotes, keywords)
    if add_combinations:
        keywords = chain(keywords, combinations(keywords, 2))
    queries = chain.from_iterable(
        map(partial(add_pages, pages=pages, title_per_page=title_per_page), keywords))
    queries = map(partial(add_year, min_year=min_year), queries)
    return queries




### main ###

def main():
    filename = output_filename_pattern.format(date=datetime.datetime.now().strftime("%y%m%d_%H%M"))
    queries = gen_queries(
        keywords, pages, min_year, add_quotes=add_quotes, add_combinations=add_combinations,
        title_per_page=title_per_page)
    titles = find_titles(queries)
    write_json_lines(titles, filename)

main()