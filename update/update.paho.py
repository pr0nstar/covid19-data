#!/usr/bin/env python3
# coding: utf-8

import io
import shutil
import pantab
import requests
import unidecode

import numpy as np
import pandas as pd

from zipfile import ZipFile


URL = 'https://opendata.arcgis.com/datasets/e88c0055ac1d4176ac25d2f068500749_0.csv'
TIMEOUT = 180
RETRY_M = 3


################################################################################
# PAHO
################################################################################


def do_fetch(_try=1):
    try:
        req = requests.get(URL, timeout=TIMEOUT)
        raw = pd.read_csv(io.BytesIO(req.content))

    except Exception as e: # :S
        if _try > RETRY_M:
            raise(e)

        do_fetch(_try + 1)

    return raw


def do_format(paho_df):
    paho_df = paho_df.set_index('OBJECTID')

    paho_df = paho_df[~paho_df.index.duplicated(keep='first')]
    paho_df['DATA_DATE'] = pd.to_datetime(paho_df['DATA_DATE'])

    paho_df = paho_df.set_index(
        ['ISO3_CODE', 'ADM1_ISOCODE', 'DATA_DATE']
    )
    paho_df = paho_df[['TOTAL_CASES', 'TOTAL_DEATHS']].sort_index()
    paho_df = paho_df[
        ~paho_df.index.get_level_values(1).str.endswith('999')
    ]

    paho_df = paho_df.groupby([
        pd.Grouper(level='ISO3_CODE'),
        pd.Grouper(level='ADM1_ISOCODE'),
        pd.Grouper(level='DATA_DATE', freq='D')
    ]).mean()

    paho_df = paho_df.unstack(level=['ISO3_CODE', 'ADM1_ISOCODE']).sort_index()
    paho_df = paho_df.round().astype('Int64')

    return paho_df


FILE_NAMES = {
    'TOTAL_CASES': 'confirmed.timeline.csv',
    'TOTAL_DEATHS': 'deaths.timeline.csv',
}
BASE_PATH = './raw/paho/'
def do_save(paho_df):
    for level in paho_df.columns.get_level_values(0).unique():
        file_name = BASE_PATH + FILE_NAMES[level]

        with open(file_name) as f:
            line_n = sum(1 for line in f)

        line_n = line_n - 4
        if line_n > len(paho_df):
            break

        paho_df[level].to_csv(file_name)


################################################################################
# Patches
################################################################################


COUNTRIES_FILE = './update/geocodes.csv'
def open_countries():
    COUNTRIES = pd.read_csv(COUNTRIES_FILE)

    COUNTRIES['geoCode'] = COUNTRIES['geoCode'].apply(
        lambda _: ('PY-0' + _[-1]) if (_.startswith('PY-') and len(_) == 4) else _
    )
    COUNTRIES = COUNTRIES.set_index(['country', 'geoName']).drop('geo', axis=1)['geoCode']

    countries_df = COUNTRIES.reset_index()
    countries_df['geoName'] = [
        unidecode.unidecode(_).lower().title() for _ in countries_df['geoName']
    ]
    countries_df = countries_df.set_index('geoName')

    return countries_df


def format_column(column_name):
    column_name = column_name.replace(' de ', ' ')
    column_name = column_name.replace(' del ', ' ')
    column_name = '_'.join(column_name.split(' '))
    column_name = unidecode.unidecode(column_name)

    return column_name.lower()


def read_hyper(data_file):
    TMP_FILE = '/tmp/t.hyper'

    with open(TMP_FILE, 'bw') as dest_file:
        shutil.copyfileobj(data_file, dest_file)

    hdf = pantab.frames_from_hyper(TMP_FILE)
    hdf = [*hdf.values()][0]

    hdf_date_columns = hdf.columns[
        hdf.columns.str.startswith('Fecha')
    ]
    hdf[hdf_date_columns] = hdf[hdf_date_columns].apply(
        lambda _: pd.to_datetime(_, dayfirst=True)
    )
    hdf.columns = [format_column(_) for _ in hdf]

    return hdf

def format_df_py(df):
    df = df.set_index('id').sort_index()

    df['sexo'] = df['sexo'].replace({'MASCULINO': 'M', 'FEMENINO': 'F'})
    df[df['edad'] > 900] = np.nan

    df['departamento'] = df['departamento'].str.lower().replace({
        'pte. hayes': 'presidente hayes'
    })

    return df


PARAGUAY_URL = 'https://public.tableau.com/workbooks/COVID19PY-Registros.twbx'
HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64)'}
def fetch_py():
    req = requests.get(PARAGUAY_URL, headers=HEADERS)

    zipfile = ZipFile(io.BytesIO(req.content))
    zipfiles = [_ for _ in zipfile.namelist() if _.endswith('hyper')]
    cases_df = deaths_df = None

    for zipfile_path in zipfiles:
        df = read_hyper(zipfile.open(zipfile_path))

        if 'fecha_obito' in df.columns:
            deaths_df = df.rename(
                columns={'departamento_residencia': 'departamento'}
            )
            deaths_df = format_df_py(deaths_df)

        elif 'fecha_confirmacion' in df.columns:
            cases_df = format_df_py(df)

        else:
            continue

    deaths_df = deaths_df[~deaths_df['departamento'].isna()]
    deaths_df['departamento'] = [
        unidecode.unidecode(_).lower().title() for _ in deaths_df['departamento']
    ]

    deaths_df['adm1_isocode'] = deaths_df['departamento'].map(
        countries_df[countries_df['country'] == 'Paraguay']['geoCode']
    )

    deaths_df = deaths_df.reset_index().groupby([
        'adm1_isocode', 'fecha_divulgacion'
    ])['id'].count()

    deaths_df = deaths_df.to_frame()
    deaths_df['country'] = 'PRY'
    deaths_df = deaths_df.set_index('country', append=True)

    deaths_df = deaths_df.unstack(
        level=['country', 'adm1_isocode']
    ).cumsum().fillna(method='ffill', axis=0)

    deaths_df = deaths_df.droplevel(0, axis=1)
    deaths_df[('PRY', 'PY-15')] = .0

    return deaths_df


def fetch_bo():
    cases_df = pd.read_csv('./processed/bolivia/cases.flat.csv')

    cases_df['fecha'] = pd.to_datetime(cases_df['fecha'])
    deaths_df = cases_df[cases_df['casos'] == 'decesos_acumulados'].set_index([
        'departamento', 'fecha'
    ])['cantidad']

    deaths_df = deaths_df.unstack(level=0)

    cbolivia_df = countries_df[countries_df['country'] == 'Bolivia']
    cbolivia_df.index = cbolivia_df.index.str.lower().str.replace(' department', '')

    deaths_df.columns = deaths_df.columns.map(cbolivia_df['geoCode'])
    deaths_df.columns = pd.MultiIndex.from_product([['BOL'], deaths_df.columns])

    deaths_df.index = deaths_df.index + pd.Timedelta(days=1)

    return deaths_df


PATCH_FILE_NAMES = {
    'TOTAL_CASES': 'confirmed.timeline.daily.patch.csv',
    'TOTAL_DEATHS': 'deaths.timeline.daily.patch.csv',
}
def do_patch():
    patch_df = fetch_bo()
    patch_df = pd.concat([patch_df, fetch_py()], axis=1)
    patch_df = patch_df.loc['2021-07-11':]

    base_patch = pd.read_csv(
        BASE_PATH + PATCH_FILE_NAMES['TOTAL_DEATHS'],
        header=[0, 1],
        index_col=0
    )
    base_patch.index = pd.to_datetime(base_patch.index)

    patch_df = pd.concat([base_patch, patch_df])
    patch_df = patch_df[~patch_df.index.duplicated(keep='last')]
    patch_df = patch_df.sort_index()

    patch_df.columns.names = ['ISO3_CODE', 'ADM1_ISOCODE']

    patch_df = patch_df.astype(dtype=pd.Int64Dtype())
    patch_df.to_csv(BASE_PATH + PATCH_FILE_NAMES['TOTAL_DEATHS'])


################################################################################
# Run
################################################################################


if __name__ == '__main__':
    paho_df = do_fetch()

    if paho_df is None or len(paho_df) < 1:
        raise('No data')

    paho_df = do_format(paho_df)
    do_save(paho_df)

    countries_df = open_countries()
    do_patch()
