#!/usr/bin/env python
# coding: utf-8

import json
import requests
import random
import traceback

import pandas as pd
import numpy as np

from bs4 import BeautifulSoup
from base64 import b64decode
from uuid import uuid4


HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64)'
}

PAHO_BASE_URL = 'https://wabi-south-central-us-api.analysis.windows.net/'
PAHO_REQUEST_URL = PAHO_BASE_URL + '/public/reports/querydata?synchronous=true'

UNICEF_BASE_URL = 'https://wabi-north-europe-api.analysis.windows.net/'
UNICEF_REQUEST_URL = UNICEF_BASE_URL + '/public/reports/querydata?synchronous=true'

LOCAL_BASE_PATH = './raw/paho/'

# Hackish powerbi fns

def get_resource_key(url):
    req = requests.get(url, headers=HEADERS)
    req = BeautifulSoup(req.text, 'html.parser')

    data_src = next(
        _ for _ in req.find_all('iframe') if 'powerbi' in _.attrs['src']
    )
    data_src = data_src.attrs['src']
    data_params = data_src.rsplit('?', 1)[1].split('&')

    embed_code = next(_ for _ in data_params if _.startswith('r='))
    embed_code = embed_code.split('=', 1)[1]

    resource_key = json.loads(b64decode(embed_code))['k']
    return resource_key


def build_query(
    connection,
    from_tables,
    select_columns,
    where_conditions=[],
    order_by=[]
):
    application_context = connection['application_context']
    model_id = connection['model_id']

    return {
      "version": "1.0.0",
      "queries": [{
        "Query": {
          "Commands": [{
            "SemanticQueryDataShapeCommand": {
              "Query": {
                "Version": 2,
                "From": from_tables,
                "Select": select_columns,
                "Where": where_conditions,
                "OrderBy": order_by
              },
              "Binding": {
                "Primary": {
                  "Groupings": [{
                    "Projections": [*range(len(select_columns))]
                  }]
                },
                "DataReduction": {
                  "DataVolume": 2,
                  "Primary": {
                    "BinnedLineSample": {},
                  }
                },
                "Version": 1
              }
            }
          }]
        },
        "CacheKey": "test_query: {}".format(random.random()),
        "QueryId": "",
        "ApplicationContext": application_context
      }],
      "cancelQueries": [],
      "modelId": model_id
    }


def build_where(table, column, op=None):
    name = "{}.{}".format(table, column)
    if op is not None:
        name = '{}({})'.format(op, name)

    return {
      "Column": {
        "Expression": {
          "SourceRef": {
            "Source": "t"
          }
        },
        "Property": column
      },
      "Name": name
    }


def inflate_data(data, columns):
    data_store = data['results'][0]['result']['data']['dsr']['DS'][0]['PH'][0]['DM0']
    inflated_data = [data_store[0]['C']]

    for data_element in data_store[1:]:
        arr = np.full(len(columns), '', dtype=object)
        mask = np.full(len(columns), False)

        if 'R' in data_element.keys():
            mask = np.array([
                bool(data_element['R'] & (1<<n)) for n in range(len(columns))
            ])

        if 'Ø' in data_element.keys():
            negative_mask = ~np.array([
                bool(data_element['Ø'] & (1<<n)) for n in range(len(columns))
            ])
            arr[negative_mask] = '--'

            mask = ~mask & negative_mask
            mask = ~mask

        arr[~mask] = data_element['C']

        inflated_data.append(arr)

    inflated_data = pd.DataFrame(inflated_data, columns=columns)
    return inflated_data

# PAHO/Unicef fns

PAHO_SOURCE_URL = 'https://www.paho.org/en/covax-americas'
PAHO_FILE_NAME = 'vaccines.covax.delivery.csv'
def paho_covax():
    # Key
    resource_key = get_resource_key(PAHO_SOURCE_URL)

    headers = HEADERS.copy()
    headers['X-PowerBI-ResourceKey'] = resource_key
    headers['RequestId'] = str(uuid4())

    # Query
    CONNECTION = {
        'application_context': {
            "DatasetId": "0f759363-1b4d-483c-b9cc-3e52aacfea34",
            "Sources": [{
                "ReportId": "ba763b1a-fa9a-4b96-a196-54365d0e4d1e",
                "VisualId": "6c9da183a30da72e7144"
            }]
        },
        'model_id': 3867677
    }

    TABLE = "Purchase Orders"
    FROM_TABLES = [{
      "Name": "t", "Entity": TABLE, "Type": 0
    }]

    COLUMNS = [
        'Country', 'Supplier', 'Buyer', 'New Arrival Date', 'Hour Status'
    ]
    SELECT_COLUMNS = [build_where(TABLE, _) for _ in COLUMNS]
    SELECT_COLUMNS.append(
        build_where(TABLE, 'Quantity', 'Sum')
    )

    QUERY = build_query(
        CONNECTION, FROM_TABLES, SELECT_COLUMNS, [], []
    )

    # Request
    data = requests.post(
        PAHO_REQUEST_URL, json=QUERY, headers=headers
    )
    data = data.json()

    # Inflate
    inflated_data = inflate_data(data, columns=COLUMNS + ['Quantity'])
    inflated_data['New Arrival Date'] = pd.to_datetime(
        inflated_data['New Arrival Date'], unit='ms', errors='coerce'
    )
    inflated_data = inflated_data.replace('', np.nan).interpolate(method='ffill')
    inflated_data['Quantity'] = inflated_data['Quantity'].astype(int)

    inflated_data = inflated_data.sort_values('New Arrival Date')
    inflated_data = inflated_data.reset_index(drop=True)

    inflated_data.to_csv(LOCAL_BASE_PATH + PAHO_FILE_NAME, index=False)

    return inflated_data


def unicef_supply_deals(headers):
    # Query
    CONNECTION = {
        'application_context': {
            "DatasetId": "9cf2f5a8-f597-4503-ae92-4f4c24f06b80",
            "Sources": [{
                "ReportId": "3abfe7a3-785e-4e21-b219-9da3b818669c",
                "VisualId": "c97d36445fe8819945d7"
            }]
        },
        'model_id': 12516632
    }

    TABLE = "mod Supply Deals View Table"
    FROM_TABLES = [{
      "Name": "t", "Entity": TABLE, "Type": 0
    }]

    COLUMNS = [
        'Manufacturer', 'Recipient', 'ISO', 'Source URL', 'Deal Date',
        'Vaccine Name', 'Committed $', 'Secured Doses', 'Optioned Doses',
        'Status', 'Vaccine Developer', 'Deal Type', 'Distributor'
    ]
    SELECT_COLUMNS = [build_where(TABLE, _) for _ in COLUMNS]

    QUERY = build_query(
        CONNECTION, FROM_TABLES, SELECT_COLUMNS, [], []
    )

     # Request
    data = requests.post(
        UNICEF_REQUEST_URL, json=QUERY, headers=headers
    )
    data = data.json()

    # Inflate
    inflated_data = inflate_data(data, columns=COLUMNS)
    inflated_data['Deal Date'] = pd.to_datetime(
        inflated_data['Deal Date'], errors='coerce'
    )
    inflated_data = inflated_data.replace('', np.nan).interpolate(method='ffill')

    inflated_data = inflated_data.sort_values('Deal Date')
    inflated_data = inflated_data.reset_index(drop=True)

    return inflated_data


def unicef_donations(headers):
    # Query
    CONNECTION = {
        'application_context': {
            "DatasetId": "9cf2f5a8-f597-4503-ae92-4f4c24f06b80",
            "Sources": [{
                "ReportId": "3abfe7a3-785e-4e21-b219-9da3b818669c",
                "VisualId": "b85e5d3003879b5ad1ca"
            }]
        },
        'model_id': 12516632
    }

    TABLE = "mod Donation Deliveries View Table"
    FROM_TABLES = [{
      "Name": "t", "Entity": TABLE, "Type": 0
    }]

    COLUMNS = [
        'Recipient', 'Recipient ISO', 'Vaccine Name', 'Manufacturer',
        'Source URL', 'Vaccine Developer', 'Donor', 'Donor ISO', 'Update Date', 'Doses'
    ]
    SELECT_COLUMNS = [build_where(TABLE, _) for _ in COLUMNS]

    QUERY = build_query(
        CONNECTION, FROM_TABLES, SELECT_COLUMNS, [], []
    )

     # Request
    data = requests.post(
        UNICEF_REQUEST_URL, json=QUERY, headers=headers
    )
    data = data.json()

     # Inflate
    inflated_data = inflate_data(data, columns=COLUMNS)
    inflated_data['Update Date'] = pd.to_datetime(
        inflated_data['Update Date'], unit='ms', errors='coerce'
    )
    inflated_data = inflated_data.replace('', np.nan).interpolate(method='ffill')

    inflated_data = inflated_data.sort_values('Update Date')
    inflated_data = inflated_data.reset_index(drop=True)

    return inflated_data


UNICEF_SOURCE_URL = 'https://www.unicef.org/supply/covid-19-vaccine-market-dashboard'
UNICEF_DONATIONS_FILE_NAME = 'vaccines.donations.csv'
UNICEF_SUPPLY_DEALS_FILE_NAME = 'vaccines.supply.deals.csv'
def unicef_covax():
    # Key
    resource_key = get_resource_key(UNICEF_SOURCE_URL)

    headers = HEADERS.copy()
    headers['X-PowerBI-ResourceKey'] = resource_key
    headers['RequestId'] = str(uuid4())

    donations = unicef_donations(headers)
    supply_deals = unicef_supply_deals(headers)

    donations.to_csv(LOCAL_BASE_PATH + UNICEF_DONATIONS_FILE_NAME, index=False)
    supply_deals.to_csv(LOCAL_BASE_PATH + UNICEF_SUPPLY_DEALS_FILE_NAME, index=False)


FNS = [
    unicef_covax,
    paho_covax
]
if __name__ == '__main__':
    for fn in FNS:
        print(fn.__name__)

        try:
            fn()
        except Exception as e:
            traceback.print_exc()
