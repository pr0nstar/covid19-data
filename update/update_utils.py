#!/usr/bin/env python
# coding: utf-8

import re
import time
import json
import random
import requests
import demjson
import unidecode

from bs4 import BeautifulSoup
import pandas as pd


TIMEOUT = 90
RETRY_M = 5
SLEEP_T = 2

# Geo


PREFIX = [
    'departamento', 'departament',
    'provincia', 'province',
    'estado', 'state',
    'region'
]
RE_PREFIX = re.compile(r'\b(?:{})\b'.format('|'.join(PREFIX)))
RE_ARTICLE = re.compile(r'^ *(de|del|of) ')
GEO_URL = 'https://raw.githubusercontent.com/esosedi/3166/master/data/iso3166-2.json'
def fetch_geocodes(trim_admin_level=True):
    geo_data = requests.get(GEO_URL)
    geo_data = json.loads(geo_data.content)

    # Patch Ñuble
    geo_data['CL']['regions'].append(
        {'name': 'Ñuble Region', 'iso': 'NB', 'names': {'geonames': 'Ñuble'}}
    )
    # Patch CABA - AR
    caba_region = next(_ for _ in geo_data['AR']['regions'] if _['iso'] == 'C')
    caba_region['names'] = {
        'geonames': 'CABA',
        'es': 'Ciudad Autonoma de Buenos Aires',
        'en': 'Autonomous City of Buenos Aires'
    }
    # Patch Lima - PE
    geo_data_pe = geo_data['PE']
    lima_province_idx = next(
        _ for _, __ in enumerate(geo_data_pe['regions']) if __['iso'] == 'LMA'
    )
    lima_province = geo_data_pe['regions'].pop(lima_province_idx)
    lima_province['names'] = {
        'geonames': 'Lima Metropolitana',
        'es': 'Provincia de Lima',
        'en': 'Lima Province'
    }
    geo_data_pe['regions'].append(lima_province)

    lima_region = next(_ for _ in geo_data['PE']['regions'] if _['iso'] == 'LIM')
    lima_region['names']['geonames'] = 'Lima'

    iso_level_0 = {}
    iso_level_1 = pd.DataFrame([])

    for geo_key in geo_data.keys():
        iso_level_0[geo_key] = geo_data[geo_key]['names'].values()

        geo_names_level_1 = {
            '{}-{}'.format(geo_key, _['iso']): [*_['names'].values()] for _ in geo_data[geo_key]['regions']
        }
        geo_names_level_1 = pd.DataFrame.from_dict(geo_names_level_1, orient='index')
        geo_names_level_1 = geo_names_level_1.fillna('')

        iso_level_1 = pd.concat([iso_level_1, geo_names_level_1])

    iso_level_0 = pd.DataFrame.from_dict(iso_level_0, orient='index')
    iso_geo_names = iso_level_1.fillna('')
    geo_names = iso_geo_names.stack().droplevel(1).reset_index()

    geo_names.columns = ['geocode', 'name']
    geo_names['name'] = geo_names['name'].map(
        unidecode.unidecode
    ).str.lower()

    if trim_admin_level:
        geo_names['name'] = geo_names['name'].str.replace(
            RE_PREFIX, ''
        ).str.replace(
            RE_ARTICLE, ''
        )

    geo_names['name'] = geo_names['name'].str.strip()
    geo_names = geo_names[geo_names['name'] != ''].set_index('name')

    return iso_level_0, iso_geo_names, geo_names


# connections stuff


def do_request(URL, data=None, _try=0, **kwargs):
    if data is not None:
        req_fn = requests.post
    else:
        req_fn = requests.get

    try:
        return req_fn(URL, data=data, **kwargs)

    except Exception as e:
        if _try > RETRY_M:
            raise(e)

    time.sleep(_try * SLEEP_T)
    return do_request(URL, data=data, _try=_try + 1, **kwargs)


def setup_connection(BASE_URL):
    req = requests.get('https://www.proxydocker.com/es/proxylist/country/Bolivia')
    html = BeautifulSoup(req.content, 'html.parser')

    meta = html.findChild('meta', attrs={'name': '_token'})
    token = meta.attrs['content']

    cookies = req.headers['Set-Cookie']
    cookies = [cookie.split(';')[0] for cookie in cookies.split(',')]
    cookies = [_ for _ in cookies if '=' in _]

    PROXY_TYPES = {
        '1': 'http',
        '2': 'https',
        '12': 'https',
        '3': 'socks4',
        '4': 'socks5'
    }

    proxy_data = {
        'token': token,
        'country': 'Bolivia',
        'city': 'all',
        'state': 'all',
        'port': 'all',
        'type': 'all',
        'anonymity': 'all',
        'need': 'all',
        'page': 1
    }
    proxies = []

    for page in range(1, 3):
        proxy_data['page'] = page
        req = requests.post(
            'https://www.proxydocker.com/es/api/proxylist/',
            data=proxy_data,
            headers={
                'Cookie': ';'.join(cookies)
            }
        )

        payload = req.json()
        if 'proxies' in payload and len(payload['proxies']) > 0:
            proxies.extend(payload['proxies'])
        else:
            break

    proxies = [(
        'https' if '2' in _['type'] else 'http',
        '{}://{}:{}'.format(PROXY_TYPES[_['type']], _['ip'], _['port'])
    ) for _ in  proxies if _['type'] in PROXY_TYPES.keys()]

    random.shuffle(proxies)
    print('testing {} proxies'.format(len(proxies)))

    for proxy in proxies:
        proxy = dict([proxy])

        try:
            requests.get(BASE_URL, timeout=30, proxies=proxy)
        except Exception as e:
            continue

        return proxy


# snis asp.net


def get_inputs(soup):
    form_inputs = soup.select('input')

    return {
        finput.get('name'):finput.get('value', '') for finput in form_inputs[:-3] if (
            finput.get('name')
        )
    }


def process_request(URL, soup, cookies, data, proxy=None, raw=False):
    form_imputs = get_inputs(soup)
    form_imputs.update(data)

    req = do_request(URL, data=form_imputs, headers={
        'Cookie': ';'.join(cookies)
    }, timeout=TIMEOUT, proxies=proxy)

    if raw:
        return req.content

    try:
        content = req.content
        content = demjson.decode(content[int(content[:3]) + 4:][7:-1])

        content = content['result'][0]
        content = content.split('|')

        content_offset = int(content[1].split(',')[0])
        content_name = content[2][:content_offset]
        content = content[2][content_offset:]

        soup_update = BeautifulSoup(content, 'html.parser')

        parent_el = soup.select_one('#' + content_name)
        parent_el = parent_el.select_one(
            '#' + next(soup_update.children).get('id')
        ).parent

        for input_update in soup_update.select('input'):
            input_id = input_update.get('id')

            if not input_id:
                continue

            target_el = soup.select_one('#' + input_id)

            if not target_el:
                target_el = soup.new_tag(name='input')
                parent_el.append(target_el)

            target_el.attrs = input_update.attrs

    except Exception as e:
        soup = BeautifulSoup(req.content, 'html.parser')

    return soup
