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


URL = 'http://reportes-siahv.minsalud.gob.bo/Reporte_Dinamico_DG.aspx'
TIMEOUT = 61
RETRY_M = 2
SLEEP_T = 3


COL_GROUP_ID = 'ctl00$ContenidoPrincipal$pivotReporteDG'
COL_ADD = 'ctl00_ContenidoPrincipal_pivotReporteDG_pgHeader{}'
COL_ORDER = 'ctl00_ContenidoPrincipal_pivotReporteDG_sortedpgHeader{}'

COL_ADDITIONS = [
    (2, 3), (7, 2), (8, 7), (9, 8), (10, 9), (11, 10), (12, 11)
]


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

    data_df = data_df.iloc[:, [0, 2, 1] + [*range(3, len(data_df.columns))]]
    data_df = data_df.fillna(method='ffill')

    data_df = data_df[
        ~data_df.iloc[:, :-1].apply(lambda _: _.str.contains('Total')).any(axis=1)
    ]

    data_df.columns = data_df.iloc[0]
    data_df = data_df.iloc[1:]

    data_df = data_df.set_index(['Departamento', 'Municipio', 'Establecimiento'])
    data_df['Dato Total'] = data_df['Dato Total'].astype(int)

    return data_df


def fetch_data(soup, dept_code):
    soup = do_request(soup, cookies, {
        '__EVENTTARGET': 'ctl00$ContenidoPrincipal$ddl_sedes',
        'ctl00$ContenidoPrincipal$ScriptManager1': (
            'ctl00$ContenidoPrincipal$UpdatePanel1|ctl00$ContenidoPrincipal$BTNGenerar'
        ),
        'ctl00$ContenidoPrincipal$BTNGenerar': 'Generar',
        'ctl00$ContenidoPrincipal$ddl_sedes': dept_code,
    })

    if 'No hay Datos' in soup.text:
        return

    content = do_request(soup, cookies, {
        'ContenidoPrincipal_ASPxComboBox1_VI': 1,
        'ctl00$ContenidoPrincipal$ASPxComboBox1': 'Excel',
        'ctl00$ContenidoPrincipal$ASPxComboBox1$DDD$L': 1,
        'ctl00$ContenidoPrincipal$ASPxButton1': '',
    }, raw=True)

    data_df = pd.read_excel(io.BytesIO(content), header=None)
    data_df = parse_df(data_df)

    data_df.index.names = [_.lower() for _ in data_df.index.names]
    data_df.columns.names = ['']

    data_df.columns = [_.lower() for _ in data_df.columns]

    return data_df

if __name__ == '__main__':
    req = requests.get(URL, timeout=TIMEOUT)
    soup = BeautifulSoup(req.content, 'html.parser')

    cookies = req.headers['Set-Cookie']
    cookies = [cookie.split(';')[0] for cookie in cookies.split(',')]

    # Agrega municipio y causas (cie)
    for col_add, col_order in COL_ADDITIONS:
        col_add = COL_ADD.format(col_add)
        col_order = COL_ORDER.format(col_order)

        soup = do_request(soup, cookies, {
            '__CALLBACKID': COL_GROUP_ID,
            '__CALLBACKPARAM': '|'.join(['c0:D', col_add, col_order, 'false'])
        })

    # Selecciona 2021
    soup = do_request(soup, cookies, {
        '__EVENTTARGET': 'ctl00$ContenidoPrincipal$ddl_gestion',
        'ctl00$ContenidoPrincipal$ScriptManager1': (
            'ctl00$ContenidoPrincipal$UpdatePanel1|ctl00$ContenidoPrincipal$ddl_gestion'
        ),
        'ctl00$ContenidoPrincipal$ddl_gestion': 7,
    })

    death_df = pd.DataFrame([])

    for dept_code in range(1, 10):
        data_df = fetch_data(soup, dept_code)
        death_df = pd.concat([death_df, data_df])

    death_df.to_csv(
        './raw/bolivia/snis/siahv/defuncion.general/{}.csv'.format(
            pd.to_datetime('today').strftime('%Y-%m-%d')
        )
    )
