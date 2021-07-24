import os
import re
import tabula
import requests
import unidecode
import traceback

import numpy as np
import pandas as pd

from bs4 import BeautifulSoup


################################################################################
# Extra
################################################################################


def format_col(col):
    if col is None or col is np.nan:
        return ''

    elif type(col) == tuple:
        return tuple(format_col(_) for _ in col)

    col = re.sub('\(.*\)', '', col)
    col = col.replace('\r', ' ')
    col = col.lower().strip()
    col = unidecode.unidecode(col)

    return col.replace(' ', '_')


def format_index(idx):
    idx = idx.str.lower()
    idx = idx.map(unidecode.unidecode)

    if type(idx.name) == tuple:
        idx.name = idx.name[0]

    idx.name = idx.name.lower()

    return idx


################################################################################
# Vacunaciones
################################################################################


def parse_vaccination_by_dose(vaccine_df):
    vaccine_df.iloc[:2] = vaccine_df.iloc[:2].fillna(method='ffill')

    vaccine_df.columns = vaccine_df.iloc[1]
    vaccine_df.columns = vaccine_df.columns.map(format_col)
    vaccine_df = vaccine_df.iloc[2:]

    vaccine_df.index = vaccine_df.iloc[:, 0]
    vaccine_df.index = format_index(vaccine_df.index)
    vaccine_df = vaccine_df.iloc[:, 1:]

    vaccine_df = vaccine_df.loc[
        :, ~vaccine_df.apply(lambda _: _.str.contains('%').any())
    ]
    vaccine_df = vaccine_df.loc[~vaccine_df.index.str.contains('bolivia')]

    vaccine_df = vaccine_df.fillna('0').astype(str)
    vaccine_df = vaccine_df.apply(
        lambda _: _.str.replace('.', '', regex=False).str.replace(',', '', regex=False)
    )
    vaccine_df = vaccine_df.astype(int)

    # Segundo filtro de columnas (al gas)
    vaccine_df_columns = (
        vaccine_df.columns.str.contains('entregadas') |
        vaccine_df.columns.str.contains('suministradas')
    )
    vaccine_df = vaccine_df.loc[:, vaccine_df_columns]
    vaccine_df.columns = pd.MultiIndex.from_product([
        ['total'], vaccine_df.columns
    ])
    vaccine_df.columns.names = ('vacuna_fabricante', 'dosis')

    return vaccine_df


def parse_vaccination_by_manufacturer(vaccine_df):
    totals = np.concatenate([
        [True], (vaccine_df.iloc[1, 1:-1].str.lower() == 'total').values, [True]
    ])

    vaccine_df.iloc[0, 1:] = np.repeat(
        (vaccine_df.iloc[0].dropna()[1:]).values,
        np.diff(np.where(totals))[0]
    )

    vaccine_df.columns = pd.MultiIndex.from_frame(vaccine_df.iloc[:2].T)
    vaccine_df.columns = vaccine_df.columns.map(format_col)
    vaccine_df.columns.names = ('vacuna_fabricante', 'dosis')
    vaccine_df = vaccine_df.iloc[2:]

    vaccine_df.index = vaccine_df.iloc[:, 0]
    vaccine_df.index = format_index(vaccine_df.index)
    vaccine_df = vaccine_df.iloc[:, 1:]

    vaccine_df = vaccine_df.loc[
        :, ~vaccine_df.columns.get_level_values(1).str.contains('total')
    ]
    vaccine_df = vaccine_df.loc[~vaccine_df.index.str.contains('bolivia')]

    vaccine_df = vaccine_df.fillna('0').astype(str)
    vaccine_df = vaccine_df.apply(
        lambda _: _.str.replace('.', '', regex=False).str.replace(',', '', regex=False)
    )
    vaccine_df = vaccine_df.astype(int)

    return vaccine_df


def parse_vaccination(path, post_date):
    vaccine_dfs = tabula.read_pdf(
        path,
        pages='all',
        lattice=True,
        options='--use-line-returns',
        pandas_options={'header': None}
    )
    df = pd.DataFrame([])

    for vaccine_df in vaccine_dfs:
        if len(vaccine_df.dropna()) < 9:
            continue

        vaccine_header = vaccine_df.iloc[0].fillna('').str.lower()

        if 'departamento' not in vaccine_header.values:
            continue

        elif vaccine_header.str.contains('entregadas').any():
            vaccine_df = parse_vaccination_by_dose(vaccine_df)

        elif all(_ in vaccine_header.values for _ in ['sinopharm', 'astrazeneca', 'pfizer']):
            vaccine_df = parse_vaccination_by_manufacturer(vaccine_df)

        else:
            print('DATOS NO PROCESADOS!')
            print(vaccine_df.head(5))

            continue

        vaccine_df = vaccine_df.unstack().rename('cantidad').to_frame()
        vaccine_df = pd.concat({post_date: vaccine_df}, names=['fecha'])

        df = pd.concat([df, vaccine_df])

    return df


################################################################################
# Casos
################################################################################


INDEX_ORDER = [
    'la paz',
    'cochabamba',
    'santa cruz',
    'oruro',
    'potosi',
    'tarija',
    'chuquisaca',
    'beni',
    'pando'
]
def parse_cases(path, post_date):
    cases_dfs = tabula.read_pdf(
        path,
        lattice=True,
        pages=1,
        area=(5, 5, 30, 50),
        relative_area=True
    )
    cases_df = cases_dfs[0]

    cases_df.columns = cases_df.columns.map(format_col)
    cases_df.columns.name = 'casos'
    cases_df.index = cases_df.iloc[:, 0]

    cases_df = cases_df.iloc[:, 1:]
    cases_df.index = format_index(cases_df.index)

    forbidden_columns = ['%', '100.000', '202']
    cases_df = cases_df[
        [_ for _ in cases_df.columns if not any(__ in _ for __ in forbidden_columns)]
    ]
    cases_df = cases_df.loc[~cases_df.index.str.contains('bolivia')]

    cases_df = cases_df.astype(str).applymap(
        lambda _: _.replace('.000', '').replace('.', '').replace(',', '')
    ).astype(int)

    cases_df = cases_df.loc[INDEX_ORDER]
    cases_df = cases_df.unstack().rename('cantidad').to_frame()
    cases_df = pd.concat({post_date: cases_df}, names=['fecha'])

    return cases_df


################################################################################
# Actualizacion
################################################################################


def do_merge(df, path):
    if os.path.isfile(path):
        store_df = pd.read_csv(path)

        store_df['fecha'] = pd.to_datetime(store_df['fecha'])
        store_df = store_df.set_index(df.index.names)
    else:
        store_df = pd.DataFrame([])

    df = pd.concat([store_df, df])
    df = df[~df.index.duplicated(keep='last')]

    df = df.sort_index()

    df.to_csv(path)


def do_update(fn, post_date, path):
    try:
        df = fn(TEMPORAL_FILE, post_date)
        do_merge(df, path)
    except Exception as e:
        traceback.print_exc()


BASE_URL = 'https://www.unidoscontraelcovid.gob.bo/index.php/wp-json/wp/v2/posts?categories=50'
TEMPORAL_FILE = '/tmp/temporal.pdf'

VACCINES_FILE = './processed/bolivia/vaccinations.flat.csv'
CASES_FILE = './processed/bolivia/cases.flat.csv'

TIMEOUT = 180
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64)'
}

if __name__ == '__main__':
    cdata = requests.get(BASE_URL, headers=HEADERS, timeout=TIMEOUT)
    latest_posts = cdata.json()

    for latest_post in latest_posts[:2]:
        latest_post = requests.get(
            latest_post['link'], headers=HEADERS, timeout=TIMEOUT
        )
        latest_post = BeautifulSoup(latest_post.content, 'html.parser')

        post_title = latest_post.findChild('h1', {'class': 'entry-title'})
        post_title = unidecode.unidecode(post_title.text).lower()
        print(post_title)

        post_links = latest_post.findChild('div', {'class': 'entry-content'})
        post_links = post_links.find_all('a')

        post_attachment = [_.attrs['href'] for _ in post_links]
        post_attachment = next(_ for _ in post_attachment if _.endswith('pdf'))

        post_attachment = requests.get(
            post_attachment, headers=HEADERS, timeout=TIMEOUT
        )

        with open(TEMPORAL_FILE, 'wb') as f:
            f.write(post_attachment.content)

        post_date = post_title.rsplit(' ', 1)[1]
        post_date = pd.to_datetime(post_date, dayfirst=True)

        if 'vacunacion' in post_title:
            do_update(
                parse_vaccination,
                post_date,
                VACCINES_FILE
            )

        elif 'casos' in post_title:
            do_update(
                parse_cases,
                post_date,
                CASES_FILE
            )
