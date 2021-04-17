#!/usr/bin/env python
# coding: utf-8

import io
import requests
import demjson
import unidecode

import numpy as np
import pandas as pd

from bs4 import BeautifulSoup
from itertools import chain, product


URL = 'http://reportes-siahv.minsalud.gob.bo/Reporte_Dinamico_Covid.aspx'
TIMEOUT = 180
RETRY_M = 2
SLEEP_T = 3


COL_ADD_1 = 'ctl00_ContenidoPrincipal_pivotReporteCovid_pgHeader14'
COL_ADD_2 = 'ctl00_ContenidoPrincipal_pivotReporteCovid_pgHeader16'

COL_BEFORE_1 = 'ctl00_ContenidoPrincipal_pivotReporteCovid_sortedpgHeader13'
COL_BEFORE_2 = 'ctl00_ContenidoPrincipal_pivotReporteCovid_sortedpgHeader14'

COL_GROUP_ID = 'ctl00$ContenidoPrincipal$pivotReporteCovid'


DEPTS = {
    'chuquisaca': 1,
    'la.paz': 2,
    'cochabamba': 3,
    'oruro': 4,
    'potosi': 5,
    'tarija': 6,
    'santa.cruz': 7,
    'beni': 8,
    'pando': 9
}


AGE_MAP = {
    '0-19': ['a', 'b', 'c', 'd', 'e'],
    '20-39': ['f'],
    '40-49': ['g'],
    '50-59': ['h'],
    '>= 60': ['i']
}
CAT_AGE_MAP = dict(chain(*[product(v,(k,)) for k,v in AGE_MAP.items()]))


def get_inputs(soup):
    form_inputs = soup.select('input')

    return {
        finput.get('name'):finput.get('value', '') for finput in form_inputs[:-3] if (
            finput.get('name')
        )
    }


def do_request(soup, cookies, data, raw=False, _try=1):
    form_imputs = get_inputs(soup)
    form_imputs.update(data)

    try:
        req = requests.post(URL, data=form_imputs, headers={
            'Cookie': ';'.join(cookies)
        })

    except Exception as e: # :S
        if _try > RETRY_M:
            return

        time.sleep(_try * SLEEP_T)
        return do_request(soup, cookies, data, raw=raw, _try=_try + 1)

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


def parse_df(data_df):
    data_df = data_df.loc[3:]
    data_df = data_df.loc[:, ~data_df.T.isna().T.all(axis=0)]
    data_df = data_df.iloc[:,:-1]

    data_df.iloc[:2] = data_df.iloc[:2].fillna(method='ffill', axis=1)
    data_df = data_df[data_df.columns[~(
        data_df.iloc[0].astype(str).str.contains('Total').fillna(False) |
        data_df.iloc[1].str.contains('Total').fillna(False)
    )]]

    data_df.iloc[:, 0] = data_df.iloc[:, 0].fillna(method='ffill')
    data_df = data_df[~data_df.iloc[:, 0].str.contains('Total').fillna(False)]

    date_index = data_df.iloc[:2].apply(lambda _: '{}-{}'.format(_.iloc[0], _.iloc[1]))
    date_index = pd.to_datetime(date_index.iloc[2:])

    data_df = data_df.iloc[1:]
    data_df.iloc[0, 2:] = date_index

    data_df.columns = pd.MultiIndex.from_frame(data_df.iloc[:2].T)
    data_df = data_df.iloc[2:]

    data_df.index = data_df.iloc[:, 0]
    data_df.index.name = 'Municipio'

    data_df = data_df.fillna(0)
    data_df = data_df.iloc[:, 1:]

    df = pd.DataFrame([])

    for muni, muni_df in data_df.groupby('Municipio'):
        muni_df.index = muni_df.iloc[:, 0]
        muni_df = muni_df.iloc[:, 1:]

        fixed_df = pd.DataFrame([])

        for age, age_df in muni_df.groupby(lambda _: CAT_AGE_MAP[_[0]]):
            fixed_df[age] = age_df.sum(axis=0)

        muni = unidecode.unidecode(muni).lower()
        fixed_df.columns = pd.MultiIndex.from_product([[muni], fixed_df.columns])

        df = pd.concat([df, fixed_df], axis=1)

    return df.T


def fetch_data(soup, dept_key, dept_code):
    soup = do_request(soup, cookies, {
        '__EVENTTARGET': 'ctl00$ContenidoPrincipal$ddl_sedes',
        'ctl00$ContenidoPrincipal$ScriptManager1': (
            'ctl00$ContenidoPrincipal$UpdatePanel1|ctl00$ContenidoPrincipal$BTNGenerar'
        ),
        'ctl00$ContenidoPrincipal$BTNGenerar': 'Generar',
        'ctl00$ContenidoPrincipal$ddl_sedes': dept_code,
    })

    fecha_carga = soup.select_one('#ContenidoPrincipal_lbl_fecha').text.split(' ')[-1]
    fecha_carga = pd.to_datetime(fecha_carga, dayfirst=True)

    content = do_request(soup, cookies, {
        'ContenidoPrincipal_ASPxComboBox1_VI': 1,
        'ctl00$ContenidoPrincipal$ASPxComboBox1': 'Excel',
        'ctl00$ContenidoPrincipal$ASPxComboBox1$DDD$L': 1,
        'ctl00$ContenidoPrincipal$ASPxButton1': '',
    }, raw=True)

    data_df = pd.read_excel(io.BytesIO(content), header=None)
    data_df = parse_df(data_df)

    data_df['departamento'] = dept_key
    data_df = data_df.set_index('departamento', append=True)

    data_df.index.names = ['municipio', 'edad', 'departamento']
    data_df = data_df.reorder_levels(['departamento', 'municipio', 'edad'])

    return fecha_carga, data_df


if __name__ == '__main__':
    req = requests.get(URL, timeout=TIMEOUT * 2)
    soup = BeautifulSoup(req.content, 'html.parser')

    cookies = req.headers['Set-Cookie']
    cookies = [cookie.split(';')[0] for cookie in cookies.split(',')]

    # Agrega columna `mesDefuncion`
    soup = do_request(soup, cookies, {
        '__CALLBACKID': COL_GROUP_ID,
        '__CALLBACKPARAM': '|'.join(['c0:D', COL_ADD_1, COL_BEFORE_1, 'true'])
    })

    # Agrega columna `Gestion`
    soup = do_request(soup, cookies, {
        '__CALLBACKID': COL_GROUP_ID,
        '__CALLBACKPARAM': '|'.join(['c0:D', COL_ADD_2, COL_BEFORE_2, 'true'])
    })

    death_df = pd.DataFrame([])

    for dept_key, dept_code in DEPTS.items():
        fecha_carga, data_df = fetch_data(soup, dept_key, dept_code)
        death_df = pd.concat([death_df, data_df])

    death_df.columns.names = ['', '']
    death_df = death_df.fillna(0).astype(np.int64)
    death_df.to_csv(
        './raw/bolivia/snis/siahv/covid/{}.csv'.format(
            fecha_carga.strftime('%Y-%m-%d')
        )
    )
