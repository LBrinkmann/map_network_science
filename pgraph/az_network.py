import datetime
import requests
import json
import time
import os
from toolz import *
from functools import partial
from itertools import chain
from tools import load_json_lines, write_json_lines, load_json
from py2neo import Graph, Node, Relationship, Subgraph

g = None
# Graph("http://localhost", auth=("neo4j", "neo4j"))


ms_academic_base = "https://api.labs.cognitive.microsoft.com/academic/v1.0/{method}?"
input_filename1 = "data/ms_paper_180526_1351.jsonl"
input_filename2 = "data/ms_paper_info_l0_180526_1244.jsonl"
author_input1 = 'data/ms_paper_info_l0_180528_0921.jsonl'
author_input2 = 'data/ms_paper_info_l1_180528_0934.jsonl'
aff_input1 = 'data/ms_author_info_l0_180528_1019.jsonl'
aff_input2 = 'data/ms_author_info_l1_180528_1027.jsonl'

affs = 'data/ms_aff_info_180528_1208.jsonl'
authors = ['data/ms_author_info_l0_180528_1019.jsonl', 'data/ms_author_info_l1_180528_1027.jsonl']
papers = ['data/ms_paper_info_l0_180528_0921.jsonl', 'data/ms_paper_info_l1_180528_0934.jsonl']

output_filename_pattern1 = 'data/ms_paper_info_l0_{date}.jsonl'
output_filename_pattern2 = 'data/ms_paper_info_l1_{date}.jsonl'

author_filename_pattern1 = 'data/ms_author_info_l0_{date}.jsonl'
author_filename_pattern2 = 'data/ms_author_info_l1_{date}.jsonl'

aff_pattern = 'data/ms_aff_info_{date}.jsonl'


headers = {"Ocp-Apim-Subscription-Key": os.environ['MS_ACADEMIC_KEY'], "Content-Type": "application/json"}


paper_fields = [
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

direct = set([
    "PublishYear",
    "PublishDate",
    "CitationCount",
    "OriginalTitle",
    "NormalizedTitle",
    "OriginalVenue",
    "NormalizedVenue",
    # "FieldOfStudyIDs",
    # "Keywords"
])

author_fields = set(
    [
        "Name", "DisplayAuthorName"
    ]
)


def create_author_body(ids):
    return {
        "path": "/author",
        "author": {
            "type": "Author",
            "id": ids,
            "select": ["Name", "DisplayAuthorName", "AffiliationIDs", "PaperIDs"],
        }
    }

def create_aff_body(ids):
    return {
        "path": "/aff",
        "aff": {
            "type": "Affiliation",
            "id": ids,
            "select": ["Name"],
        }
    }


g_map = "https://maps.googleapis.com/maps/api/{method}/json?"
google_key = os.environ['GOOGLE_MAPS_KEY']
continent_map = load_json('country_continent.json')

def get_location(name):
    params = {
        'key': google_key,
        'query': name
    }
    url = g_map.format(method = 'place/textsearch')
    resp = requests.get(url=url, params=params)
    resp = resp.json()

    if len(resp["results"]):
        address = resp["results"][0]["formatted_address"]

        params = {
            'key': google_key,
            'address': address
        }
        url = g_map.format(method = 'geocode')
        resp = requests.get(url=url, params=params)
        resp = resp.json()

        country_info = next(filter(
            lambda x: set(x["types"]) == set(["country", "political"]),
            resp["results"][0]['address_components']))

        country_long_name = country_info['long_name']
        country_short_name = country_info['short_name']
        continent = continent_map[country_short_name]
        return {
            'countryName': country_long_name, 'contryShortName': country_short_name, 
            'continent': continent}
    else:
        return {}

def try_connection(method, *args, **kwargs):
    global g
    try:
        return method(*args, **kwargs, graph=g)
    except:
        g = Graph("http://localhost", auth=("neo4j", "neo4j"))
        try:
            return method(*args, **kwargs, graph=g)
        except:
            pass


def _author_to_neo(info, graph):
    print(info)
    tx = graph.begin()
    author = Node("Author", CellID=info['CellID'])

    # paper_author = [
    #     Node("PaperAuthor", CellID=hash(info['CellID'] + pid)) for pid in info['PaperIDs']
    # ]
    # papers = [Node("Paper", CellID=pid) for pid in info['PaperIDs']]
    # affilitation = [Node("Affiliation", CellID=aid) for aid in info['AffiliationIDs']]

    # authorships1 = [Relationship(p, 'HAS', pa) for p, pa in zip(papers, paper_author)]
    # authorships2 = [Relationship(pa, 'IS', author) for pa in paper_author]
    # affiliationship = [Relationship(pa, 'AFFILIATE', aff) for pa, aff in zip(paper_author, affilitation)]

    # nodes =  [author] + papers + paper_author + affilitation
    # relations = authorships1 + authorships2 + affiliationship
    nodes =  [author] 
    relations = None

    sg = Subgraph(nodes, relations)
    tx.merge(sg, primary_label=None, primary_key='CellID')
    tx.commit()

    direct_info = keyfilter(author_fields.__contains__, info)

    author.update(**direct_info)
    graph.push(author)




def _aff_to_neo(info, location, graph):
    tx = graph.begin()
    aff = Node("Affiliation", CellID=info['CellID'])
    tx.merge(aff, primary_label=None, primary_key='CellID')
    tx.commit()
    aff.update(Name=info['Name'], **location)
    graph.push(aff)    


aff_to_neo = partial(_aff_to_neo, graph=g)


def create_paper_body(ids):
    return {
        "path": "/paper1",
        "paper1": {
            "type": "Paper",
            "id": ids,
            "select": paper_fields
        }
    }


def extract_id(paper):
    try:
        return paper['ms_academic']['entities'][0]["Id"]
    except:
        return


def _to_neo(p_info, g_info, level, graph):
    print(p_info['OriginalTitle'])
    tx = graph.begin()
    paper = Node("Paper", CellID=p_info['CellID'])

    citation_paper = [Node("Paper", CellID=pid, level=level+1) for pid in p_info['CitationIDs']]
    # reference_paper = [Node("Paper", CellID=pid, level=level+1) for pid in p_info['ReferenceIDs']]

    citations = [Relationship(p, 'CITED', paper) for p in citation_paper]
    # references = [Relationship(paper, 'CITED', p) for p in reference_paper]

    # fields_of_study = [Node("FieldOfStudy", CellID=fid) for fid in p_info['FieldOfStudyIDs']]
    # paper_fos = [Relationship(paper, 'IS_ABOUT', fos) for fos in fields_of_study]

    # keywords = [Node("Keyword", CellID=hash(kw), name=kw) for kw in p_info['Keywords']]
    # paper_kw = [Relationship(paper, 'IS_ABOUT', kw) for kw in keywords]

    paper_author = [
        Node("PaperAuthor", CellID=hash(aid + p_info['CellID'])) for aid in p_info['AuthorIDs']
    ]
    authors = [Node("Author", CellID=aid) for aid in p_info['AuthorIDs']]
    affilitation = [Node("Affiliation", CellID=aid) for aid in p_info['AffiliationIDs']]

    authorships1 = [Relationship(paper, 'HAS', pa) for pa in paper_author]
    authorships2 = [Relationship(pa, 'IS', a) for pa, a in zip(paper_author, authors)]
    affiliationship = [Relationship(pa, 'AFFILIATE', aff) for pa, aff in zip(paper_author, affilitation)]

    # nodes =  citation_paper + reference_paper + [paper] + fields_of_study + keywords + paper_author + authors
    # relations = authorships1 + authorships2 + affiliationship + citations + references + paper_fos + paper_kw

    nodes =  [paper] + paper_author + authors + citation_paper + affilitation
    relations = authorships1 + authorships2 + affiliationship + citations

    sg = Subgraph(nodes, relations)
    tx.merge(sg, primary_label=None, primary_key='CellID')
    tx.commit()

    direct_info = keyfilter(direct.__contains__, p_info)
    if g_info:
        google_info = {f'google_{k}': v for k, v in g_info.items()}
    else:
        google_info = {}

    paper.update(**direct_info, **google_info, level=level)
    graph.push(paper)

    for aff, name in zip(affilitation, p_info['NormalizedAffiliations']):
        aff.update(NormalizedName=name)
        graph.push(aff)

    for pa, seq_number in zip(paper_author, p_info['AuthorSequenceNumbers']):
        pa.update(AuthorSequenceNumbers=seq_number)
        graph.push(pa)      

to_neo = partial(try_connection, method=_to_neo)
author_to_neo = partial(try_connection, method=_author_to_neo)


def post_ms_academic(url, headers, method, body, **params):
    # time.sleep(1)
    resp = requests.post(url=url.format(method=method), json=body, headers=headers, params=params)
    return resp.json()


def _get_aff_info(ids):
    body = create_aff_body(ids)
    result = get_ms_ac_paper_info(body=body)
    print(result)
    return (res[0] for res in result['Results'])


def _get_author_info(ids):
    body = create_author_body(ids)
    result = get_ms_ac_paper_info(body=body)
    return (res[0] for res in result['Results'])


def get_paper_info(ids):
    body = create_paper_body(ids)
    result = get_ms_ac_paper_info(body=body)

    return (res[0] for res in result['Results'])


get_ms_ac_paper_info = partial(
    post_ms_academic, url=ms_academic_base, method='graph/search', mode='json', headers=headers)

def add_papers_to_graph(papers, level):
    for p in papers:
        to_neo(p_info=p['msAcademicInfo'], g_info=p['googleScholarInfo'], level=level)

def add_aff_to_graph(recs):
    for r in recs:
        aff_to_neo(info=r['msAcademicInfo'], location=r['location'])

def add_author_to_graph(recs):
    for r in recs:
        author_to_neo(info=r['msAcademicInfo'])


def get_paper_infos(papers):
    for papers_batch in partition_all(10, papers):
        papers_batch = list(papers_batch)
        paper_ids = map(extract_id, papers_batch)
        info = get_paper_info(list(paper_ids))
        for inf, pb  in zip(info, papers_batch):
            yield {'msAcademicInfo': inf, 'googleScholarInfo': pb['googleScholarInfo']}


def get_ms_ac_info1():
    filename = output_filename_pattern1.format(date=datetime.datetime.now().strftime("%y%m%d_%H%M"))
    papers = load_json_lines(input_filename1)
    papers_filtered = filter(lambda p: len(p['ms_academic']['entities']), papers)
    paper_infos = get_paper_infos(papers_filtered)
    write_json_lines(paper_infos, filename)


def get_paper_infos2(paper_ids):
    for papers_batch in partition_all(10, paper_ids):
        papers_batch = list(papers_batch)
        info = get_paper_info(papers_batch)
        for inf  in info:
            yield {'msAcademicInfo': inf, 'googleScholarInfo': None}


def get_author_infos(author_ids):
    for author_batch in partition_all(10, author_ids):
        author_batch = list(author_batch)
        info = _get_author_info(author_batch)
        for inf  in info:
            yield {'msAcademicInfo': inf, 'googleScholarInfo': None}


def get_aff_infos(ids):
    for batch in partition_all(10, ids):
        batch = list(batch)
        info = _get_aff_info(batch)
        for inf  in info:
            location = get_location(inf['Name'])
            yield {'msAcademicInfo': inf, 'location': location}


def get_citing_papers(papers):
    for p in papers:
        for c in p['msAcademicInfo']['CitationIDs']:
            yield c

def get_citing_papers_info():
    filename = output_filename_pattern2.format(date=datetime.datetime.now().strftime("%y%m%d_%H%M"))
    papers = load_json_lines(input_filename2)
    paper_ids = get_citing_papers(papers)
    paper_infos = get_paper_infos2(paper_ids)
    write_json_lines(paper_infos, filename)


def get_authors(papers):
    for p in papers:
        for a in p['msAcademicInfo']['AuthorIDs']:
            yield a

def get_aff(authors):
    for a in authors:
        for aa in a['msAcademicInfo']['AffiliationIDs']:
            yield aa


def get_author_info():
    filename = author_filename_pattern2.format(date=datetime.datetime.now().strftime("%y%m%d_%H%M"))
    papers = load_json_lines(author_input2)
    author_ids = get_authors(papers)
    author_ids = list(set(author_ids))
    author_info = get_author_infos(author_ids)
    write_json_lines(author_info, filename)


def get_affiliation_info():
    filename = aff_pattern.format(date=datetime.datetime.now().strftime("%y%m%d_%H%M"))
    author = chain(load_json_lines(aff_input1), load_json_lines(aff_input2))
    aff_ids = get_aff(author)
    aff_ids = list(set(aff_ids))
    aff_info = get_aff_infos(aff_ids)
    write_json_lines(aff_info, filename)

affs_fn = 'data/ms_aff_info_180528_1208.jsonl'
author_fn = ['data/ms_author_info_l0_180528_1019.jsonl', 'data/ms_author_info_l1_180528_1027.jsonl']
paper_fn = ['data/ms_paper_info_l0_180528_0921.jsonl', 'data/ms_paper_info_l1_180528_0934.jsonl']

def write_to_neo():
    for level, filename in enumerate(paper_fn):
        recs = load_json_lines(filename)
        add_papers_to_graph(recs, level)

def write_aff_to_neo():
    recs = load_json_lines(affs_fn)
    add_aff_to_graph(recs)

def write_author_to_neo():
    for level, filename in enumerate(author_fn):
        recs = load_json_lines(filename)
        add_author_to_graph(recs)
