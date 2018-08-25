import docopt
import os
import logging
import pandas as pd
from toolz import partition_all, compose, keyfilter
from functools import partial
from tools import load_json_lines, write_json_lines, post_json, log_stream, key_to_pos

log = logging.getLogger(__name__)

def try_empty(func):
    def _func(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except KeyboardInterrupt:
            raise
        except BaseException as e:
            log.debug(f'Failed with: {args} {kwargs}')
            log.debug(e)
            return {}
    return _func


@try_empty
def query_nodes(p):
    return {
        'Id:ID(query)': hash((p['googleScholarInfo']['query'], p['googleScholarInfo']['min_year'])),
        'query': p['googleScholarInfo']['query'],
        'min_year': p['googleScholarInfo']['min_year']
    }


@try_empty
def paper_query_relations(p):
    return {
        ':START_ID(query)': hash((p['googleScholarInfo']['query'], p['googleScholarInfo']['min_year'])),
        ':TYPE': 'RETRIEVED',
        ':END_ID(paper)': p['ms_academic']['entities'][0]["Id"],
        'Idx': p['googleScholarInfo']['idx'],
    }


@try_empty
def paper_nodes(p):
    copy_fields = [
        "PublishYear",
        "PublishDate",
        "CitationCount",
        "OriginalTitle",
        "NormalizedTitle",
        "JournalID",
        "ConferneceID",
        "OriginalVenue",
        "NormalizedVenue",
    ]
    return {
        'Id:ID(paper)': p['info']["CellID"],
        **keyfilter(copy_fields.__contains__, p['info'])
    }


def to_df(s, extract_func):
    s = map(extract_func, s)
    s = filter(None, s)
    s_df = dataframe_from_records_in_order(s)
    s_df = s_df.dropna().drop_duplicates()
    return s_df


def dataframe_from_records_in_order(recs):
    recs = list(recs)
    keys = get_and_assert_keys(recs)
    s_df = pd.DataFrame.from_records(recs)
    s_df = s_df[keys]
    return s_df


def get_and_assert_keys(recs):
    keys = None
    for rec in recs:
        if not keys:
            keys = list(rec.keys())
        else:
            assert keys == list(rec.keys()), 'All dicts need to have same keys.'
    return keys


def to_csv(df, output_file):
    df.to_csv(output_file, index=False, header=True)
#    pd.DataFrame([df.columns]).to_csv(f'{output_file}_header.csv', index=False, header=False)


# shared logic

def gen_csv(input_file, output_file, extract_func):
    s_input = load_json_lines(input_file)
    s_df = to_df(s_input, extract_func)
    to_csv(s_df, output_file)


gen_paper_csv = partial(gen_csv, extract_func=paper_nodes)
gen_query_relations_csv = partial(gen_csv, extract_func=paper_query_relations)
gen_query_nodes_csv = partial(gen_csv, extract_func=query_nodes)


def main():
    '''
Get papers from Mircosoft Academic

Usage:
    to_csv.py (paper|query|paper_query) <input_file> <output_file>
    '''
    args = docopt.docopt(main.__doc__)
    args_clean = {arg[1:-1]: val for arg, val in args.items() if arg[0] == '<'}
    if args['paper']:
        gen_paper_csv(**args_clean)
    if args['query']:
        gen_query_nodes_csv(**args_clean)
    if args['paper_query']:
        gen_query_relations_csv(**args_clean)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG, format='[%(asctime)s] %(levelname)s@%(module)s.%(funcName)s>> %(message)s')
    main()