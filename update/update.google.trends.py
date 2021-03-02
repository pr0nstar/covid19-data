#!/usr/bin/env python
# coding: utf-8

import os
import glob
import unidecode

import pytrends.dailydata
import pytrends.request

import datetime as dt
import pandas as pd


QUERY = ['covid sintomas']


def fetch_payload(pytrend, geo_code, timeframe):
    pytrend.build_payload(
        QUERY, geo=geo_code, timeframe=timeframe, gprop=''
    )

    return pytrend.interest_over_time()


def fetch_geo_sample(pytrend, geo_code, allow_partial):
    today = dt.date.today()

    sample_df = fetch_payload(
        pytrend, geo_code, '{} {}'.format(
            str(today - pd.DateOffset(years=1))[:10], today
        )
    )

    if QUERY[0] not in sample_df.columns:
        return

    if not allow_partial:
        sample_df = sample_df[
            (sample_df['isPartial'] == 'False')|(sample_df['isPartial'] == False)
        ]

    sample_df = sample_df[QUERY]

    for month in range(1, 4):
        tdf = fetch_payload(
            pytrend, geo_code, '2020-{:02d}-01 {}'.format(month, today)
        )
        tdf = tdf.loc[sample_df.index[sample_df.index.isin(tdf.index)]]

        sample_df = pd.concat([sample_df, tdf[QUERY]], axis=1)

    return sample_df.dropna().mean(axis=1)


def fetch_dept_samples(geo_codes, data=None, allow_partial=True):
    data = data if data else {}

    for dept_name, geo_code in geo_codes.iteritems():
        sample_df = None

        try:
            pytrend = pytrends.request.TrendReq(
                retries=10, timeout=(15, 15),
            )
            sample_df = fetch_geo_sample(pytrend, geo_code, allow_partial)

        except Exception as e:
            print('errors: {}'.format(e))
            continue

        if sample_df is None:
            continue

        dept_df = data.get(dept_name, pd.DataFrame([]))
        dept_df = pd.concat([dept_df, sample_df], axis=1)
        data[dept_name] = dept_df

    return data


BASE_PATH = './raw/google/trends/'


def pickle_data(country_data):
    for country_name in country_data.keys():
        country = country_data[country_name]
        country_path = os.path.join(BASE_PATH, country_name)

        if not os.path.isdir(country_path):
            os.mkdir(country_path)

        for dept_name in country.keys():
            dept = country[dept_name]
            dept = dept.T.drop_duplicates().dropna(how='all')

            dept.columns.name = 'fecha'
            dept.index = [*range(len(dept.index))]

            dept_path = os.path.join(country_path, dept_name)
            dept.to_csv(dept_path + '.csv')


def unpickle_data():
    country_data = {}
    for country_path in glob.glob(BASE_PATH + '*'):
        country_name = os.path.basename(country_path)
        country = {}

        for dept_path in glob.glob(country_path + '/*.csv'):
            depth_name = os.path.basename(dept_path).rsplit('.', 1)[0]

            dept_df = pd.read_csv(dept_path, index_col=0)
            dept_df.columns = pd.to_datetime(dept_df.columns)

            country[depth_name] = dept_df.T

        country_data[country_name] = country

    return country_data


if __name__ == '__main__':
    COUNTRIES = pd.read_csv('./update/geocodes.csv', index_col=['country', 'geoName'])
    country_data = unpickle_data()

    for country in COUNTRIES.index.get_level_values(0).unique():
        print(country)
        country_params = COUNTRIES.loc[country]

        data = country_data.get(country, {})
        data = fetch_dept_samples(
            country_params['geoCode'], data, allow_partial=False
        )

        country_data[country] = data

    pickle_data(country_data)
