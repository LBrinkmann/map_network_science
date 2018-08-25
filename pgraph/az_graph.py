import docopt
import os
import logging
from toolz import partition_all, compose
from functools import partial
from tools import load_json_lines, write_json_lines, post_json, log_stream, key_to_pos

# azure api


ms_academic_base = "https://api.labs.cognitive.microsoft.com/academic/v1.0/{method}?"
headers = {
    "Ocp-Apim-Subscription-Key": os.environ['MS_ACADEMIC_KEY'], 
    "Content-Type": "application/json"
}

get_ms_ac_paper_info = key_to_pos(
    partial(post_json, url=ms_academic_base, method='graph/search', mode='json', headers=headers),
    'body'
)


# azure queries

def _paper_body(ids):
    return {
        "path": "/paper1",
        "paper1": {
            "type": "Paper",
            "id": ids,
            "select": [
                "PublishYear",
                "PublishDate",
                "CitationCount",
                "OriginalTitle",
                "NormalizedTitle",
                "JournalID",
                "ConferneceID",
                "OriginalVenue",
                "NormalizedVenue",
                "AuthorIDs",
                "AffiliationIDs" ,
                "NormalizedAffiliations",
                "AuthorSequenceNumbers",
                "FieldOfStudyIDs",
                "Keywords",
                "ReferenceIDs", 
                "CitationIDs"
            ]
        }
    }


def _author_body(ids):
    return {
        "path": "/author",
        "author": {
            "type": "Author",
            "id": ids,
            "select": ["Name", "DisplayAuthorName", "AffiliationIDs", "PaperIDs"],
        }
    }


def _affiliation_body(ids):
    return {
        "path": "/aff",
        "aff": {
            "type": "Affiliation",
            "id": ids,
            "select": ["Name"],
        }
    }


# compose queries with post request

_get_authors = compose(get_ms_ac_paper_info, _author_body)
_get_papers = compose(get_ms_ac_paper_info, _paper_body)
_get_affiliations = compose(get_ms_ac_paper_info, _affiliation_body)


# extract id from previous results

def _extract_paper_from_papers(papers):
    for p in papers:
        try:
            yield p['ms_academic']['entities'][0]["Id"]
        except:
            pass


def _extract_authors_from_papers(papers):
    for p in papers:
        for a in p['info']['AuthorIDs']:
            yield a


def _extract_papers_from_authors(authors):
    for p in authors:
        for a in p['info']['PaperIDs']:
            yield a


def _extract_affiliations_from_papers(papers):
    for a in papers:
        for aa in a['info']['AffiliationIDs']:
            yield aa


# shared logic

def get_x_from_y(input_file, output_file, get_func, extract_func, batch_size):
    s_input = load_json_lines(input_file)
    s_input = log_stream(s_input, name='Input')
    s_ids = extract_func(s_input)
    s_ids = list(set(s_ids))
    s_info = get_infos(s_ids, get_func=get_func, batch_size=batch_size)
    s_info = log_stream(s_info, name='Output')
    write_json_lines(s_info, output_file)


def get_infos(ids, get_func, batch_size):
    for batch in partition_all(batch_size, ids):
        batch = list(batch)
        result = get_func(batch)
        info = (res[0] for res in result)
        for inf in info:
            yield {'info': inf}


get_paper_from_papers = partial(
    get_x_from_y, get_func=_get_papers, extract_func=_extract_paper_from_papers, batch_size=10)

get_authors_from_papers = partial(
    get_x_from_y, get_func=_get_authors, extract_func=_extract_authors_from_papers, batch_size=10)

get_papers_from_author = partial(
    get_x_from_y, get_func=_get_papers, extract_func=_extract_papers_from_authors, batch_size=10)

get_affiliations_from_papers = partial(
    get_x_from_y, get_func=_get_affiliations, extract_func=_extract_affiliations_from_papers, 
    batch_size=10)

### main ###


def main():
    '''
Get papers from Mircosoft Academic

Usage:
    az_author.py (p4p|a4p|p4a|aff4p) <input_file> <output_file>
    '''
    args = docopt.docopt(main.__doc__)
    args_clean = {arg[1:-1]: val for arg, val in args.items() if arg[0] == '<'}
    if args['p4p']:
        get_paper_from_papers(**args_clean)
    if args['a4p']:
        get_authors_from_papers(**args_clean)
    if args['p4a']:
        get_papers_from_author(**args_clean)
    if args['aff4p']:
        get_affiliations_from_papers(**args_clean)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG, format='[%(asctime)s] %(levelname)s@%(module)s.%(funcName)s>> %(message)s')
    main()