#!/usr/bin/env python
# coding: utf-8

import io
import sys
import unidecode

import numpy as np
import pandas as pd

from bs4 import BeautifulSoup
from itertools import chain, product

from update_utils import *


BASE_URL = 'http://reportes-siahv.minsalud.gob.bo/'
URL = BASE_URL + 'Reporte_Dinamico_DG.aspx'

COL_GROUP_ID = 'ctl00$ContenidoPrincipal$pivotReporteDG'
COL_ADD = 'ctl00_ContenidoPrincipal_pivotReporteDG_pgHeader{}'
COL_ORDER = 'ctl00_ContenidoPrincipal_pivotReporteDG_sortedpgHeader{}'

COL_ADDITIONS = [
    (2, 3), (7, 2), (8, 7), (9, 8), (10, 9), (11, 10), (12, 11)
]


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


def fetch_data(soup, dept_code, cookies, proxy=None):
    soup = process_request(URL, soup, cookies, {
        '__EVENTTARGET': 'ctl00$ContenidoPrincipal$ddl_sedes',
        'ctl00$ContenidoPrincipal$ScriptManager1': (
            'ctl00$ContenidoPrincipal$UpdatePanel1|ctl00$ContenidoPrincipal$BTNGenerar'
        ),
        'ctl00$ContenidoPrincipal$BTNGenerar': 'Generar',
        'ctl00$ContenidoPrincipal$ddl_sedes': dept_code,
    }, proxy)

    if 'No hay Datos' in soup.text:
        return

    content = process_request(URL, soup, cookies, {
        'ContenidoPrincipal_ASPxComboBox1_VI': 1,
        'ctl00$ContenidoPrincipal$ASPxComboBox1': 'Excel',
        'ctl00$ContenidoPrincipal$ASPxComboBox1$DDD$L': 1,
        'ctl00$ContenidoPrincipal$ASPxButton1': '',
    }, proxy, raw=True)

    data_df = pd.read_excel(io.BytesIO(content), header=None)
    data_df = parse_df(data_df)

    data_df.index.names = [_.lower() for _ in data_df.index.names]
    data_df.columns.names = ['']

    data_df.columns = [_.lower() for _ in data_df.columns]

    return data_df


if __name__ == '__main__':
    # Busca un proxy que funcione
    if len(sys.argv) > 1 and '--direct' in sys.argv:
        proxy = None
    else:
        proxy = setup_connection(BASE_URL)

    if not proxy:
        print('No available proxy')
        if '--no-fail' not in sys.argv:
            exit(1)
    else:
        print(proxy)

    # Primer request
    req = do_request(URL, timeout=TIMEOUT * 2, proxies=proxy)
    soup = BeautifulSoup(req.content, 'html.parser')

    cookies = req.headers['Set-Cookie']
    cookies = [cookie.split(';')[0] for cookie in cookies.split(',')]

    # Agrega municipio y causas (cie)
    for col_add, col_order in COL_ADDITIONS:
        col_add = COL_ADD.format(col_add)
        col_order = COL_ORDER.format(col_order)

        soup = process_request(URL, soup, cookies, {
            '__CALLBACKID': COL_GROUP_ID,
            '__CALLBACKPARAM': '|'.join(['c0:D', col_add, col_order, 'false'])
        }, proxy)

    # Selecciona 2022
    soup = process_request(URL, soup, cookies, {
        '__EVENTTARGET': 'ctl00$ContenidoPrincipal$ddl_gestion',
        'ctl00$ContenidoPrincipal$ScriptManager1': (
            'ctl00$ContenidoPrincipal$UpdatePanel1|ctl00$ContenidoPrincipal$ddl_gestion'
        ),
        'ctl00$ContenidoPrincipal$ddl_gestion': 8,
    }, proxy)

    death_df = pd.DataFrame([])

    for dept_code in range(1, 10):
        data_df = fetch_data(soup, dept_code, cookies, proxy)
        death_df = pd.concat([death_df, data_df])

    death_df.to_csv(
        './raw/bolivia/snis/siahv/defuncion.general/{}.csv'.format(
            pd.to_datetime('today').strftime('%Y-%m-%d')
        )
    )
