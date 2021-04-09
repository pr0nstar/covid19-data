#!/usr/bin/env python3
# coding: utf-8

import io
import requests

import pandas as pd


URL = 'https://opendata.arcgis.com/datasets/e88c0055ac1d4176ac25d2f068500749_0.csv'
TIMEOUT = 180
RETRY_M = 3


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


if __name__ == '__main__':
    paho_df = do_fetch()

    if paho_df is None:
        raise('No data')

    paho_df = do_format(paho_df)
    do_save(paho_df)
