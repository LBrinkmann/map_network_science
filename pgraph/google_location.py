
import docopt
import requests
import os
import logging
from tools import load_json
from pkg_resources import resource_filename
from toolz import partition_all, compose
from functools import partial
from tools import load_json_lines, write_json_lines, post_json, log_stream, key_to_pos

# google location api

g_map = "https://maps.googleapis.com/maps/api/{method}/json?"
google_key = os.environ['GOOGLE_MAPS_KEY']
continent_map = load_json(resource_filename(__name__, 'country_continent.json'))


def _extract_name_from_affiliations(affiliations):
    for a in affiliations:
        yield a['info']


def get_location(info):
    params = {
        'key': google_key,
        'query': info['Name']
    }
    url = g_map.format(method='place/textsearch')
    resp = requests.get(url=url, params=params)
    resp = resp.json()

    if len(resp["results"]):
        address = resp["results"][0]["formatted_address"]

        params = {
            'key': google_key,
            'address': address
        }
        url = g_map.format(method='geocode')
        resp = requests.get(url=url, params=params)
        resp = resp.json()

        try:
            city_info = next(filter(
                lambda x: set(x["types"]) == set(["locality", "political"]),
                resp["results"][0]['address_components']))
            city_long_name = city_info['long_name']
            city_short_name = city_info['short_name']

            country_info = next(filter(
                lambda x: set(x["types"]) == set(["country", "political"]),
                resp["results"][0]['address_components']))

            country_long_name = country_info['long_name']
            country_short_name = country_info['short_name']
            continent = continent_map[country_short_name]
            return {
                'cityName': city_long_name, 'cityShortName': city_short_name,
                'countryName': country_long_name, 'contryShortName': country_short_name,
                'continent': continent
            }
        except:
            return {}
    else:
        return {}


# shared logic

def get_x_from_y(input_file, output_file, get_func, extract_func):
    s_input = load_json_lines(input_file)
    s_input = log_stream(s_input, name='Input')
    infos = extract_func(s_input)
    s_info = get_infos(infos, get_func=get_func)
    s_info = log_stream(s_info, name='Output')
    write_json_lines(s_info, output_file)


def get_infos(infos, get_func):
    for info in infos:
        result = get_func(info)
        yield {'input': info, 'info': result}


get_location_from_affiliation = partial(
    get_x_from_y, get_func=get_location, extract_func=_extract_name_from_affiliations)


### main ###


def main():
    '''
Get papers from Mircosoft Academic

Usage:
    google_location.py <input_file> <output_file>
    '''
    args = docopt.docopt(main.__doc__)
    args_clean = {arg[1:-1]: val for arg, val in args.items() if arg[0] == '<'}
    get_location_from_affiliation(**args_clean)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG, format='[%(asctime)s] %(levelname)s@%(module)s.%(funcName)s>> %(message)s')
    main()