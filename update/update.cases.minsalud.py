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
    col = col.replace('\n', ' ')
    col = col.lower().strip()
    col = unidecode.unidecode(col)

    return col.replace(' ', '_')


def format_index(idx):
    idx = idx.str.lower().str.replace(r'[\t\r\n]+', ' ', regex=True)
    idx = idx.map(unidecode.unidecode)
    idx = idx.str.replace(r'almacen[ ]*pai', 'almacen', regex=True)

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
            print('Err: Datos no procesados!')
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
def parse_cases_df(cases_df):
    cases_df.columns = cases_df.columns.map(format_col)
    cases_df.columns.name = 'casos'
    cases_df.index = cases_df.iloc[:, 0]

    cases_df = cases_df.iloc[:, 1:]
    cases_df.index = format_index(cases_df.index)

    thousands = cases_df.iloc[:, 0].str.replace(r'[0-9]+', '', regex=True)
    thousands = set(thousands.apply(list).sum())

    forbidden_columns = ['%', '100.000', '202', 's.e.', '_se_']
    cases_df = cases_df[
        [_ for _ in cases_df.columns if not any(__ in _ for __ in forbidden_columns)]
    ]
    cases_df = cases_df.loc[~cases_df.index.str.contains('bolivia')]

    if thousands == {'.'}:
        cases_df = cases_df.astype(str).applymap(
            lambda _: _.replace('.', '').replace(',', '.')
        )
    else:
        cases_df = cases_df.astype(str).applymap(
            lambda _: _.replace(',', '')
        )

    return cases_df

def parse_cases(path, post_date):
    ret_dfs = pd.DataFrame([])
    tabula_opts = {
        'lattice': True,
        'area': (5, 5, 25, 50),
        'relative_area': True,
        'pandas_options': {'dtype': 'str'}
    }

    # Try process cases/tests, if error revert to cases only
    try:
        cases_dfs = tabula.read_pdf(path, pages=[1, 2], **tabula_opts)
    except:
        print('handled!')
        cases_dfs = tabula.read_pdf(path, pages=1, **tabula_opts)

    for cases_df in cases_dfs:
        if len(cases_df) < 9:
            continue

        cases_df = parse_cases_df(cases_df)

        if 'casos_nuevos' in cases_df.columns:
            cases_df = cases_df.astype(int)

        elif 'indice_de_positividad' in cases_df.columns:
            cases_df = cases_df.astype(float)

            if (cases_df < 2).all().all():
                cases_df = 100 * cases_df

            cases_df = cases_df.astype(int)

        else:
            print('Err: Datos no procesados!')
            print(cases_df.head(5))

            continue

        cases_df = cases_df.loc[INDEX_ORDER]
        cases_df = cases_df.unstack().rename('cantidad').to_frame()

        ret_dfs = pd.concat([ret_dfs, cases_df])

    ret_dfs = pd.concat({post_date: ret_dfs}, names=['fecha'])

    return ret_dfs


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


def do_synk_vaccionations():
    # read flat + format
    df = pd.read_csv(VACCINES_FILE)
    df['fecha'] = pd.to_datetime(df['fecha'])

    df = df[df['vacuna_fabricante'] != 'total'].copy()

    df.loc[df['dosis'].str.contains('1ra'), 'dosis'] = 'Primera'
    df.loc[df['dosis'].str.contains('2da'), 'dosis'] = 'Segunda'
    df.loc[df['dosis'].str.contains('unica'), 'dosis'] = 'Unica'

    df['departamento'] = df['departamento'].str.title()

    df = df.set_index([
        'fecha', 'departamento', 'vacuna_fabricante', 'dosis'
    ])['cantidad']
    df = df.unstack(level=['departamento', 'vacuna_fabricante', 'dosis'])

    df = df.T.sort_index().T
    df = df.groupby(level=['departamento', 'dosis'], axis=1).sum(min_count=1)

    # read storage + merge
    store_df = pd.read_csv(VACCINES_SQUARE_FILE, header=[0, 1], index_col=0)
    store_df.index = pd.to_datetime(store_df.index)

    df = pd.concat([store_df, df], join='inner')
    df = df[~df.index.duplicated(keep='last')]
    df = df.sort_index()

    # test
    negative_test = df.fillna(method='ffill').diff().fillna(False) < 0
    if negative_test.any().any():
        print(negative_test.index[negative_test.any(axis=1)])
        raise(Exception('Negative value found'))

    date_test = df.index.to_series().diff().dt.days > 1
    if date_test.any():
        df = df.resample('D').mean()
        print('Missing data')

    # store
    df = df.astype(pd.Int64Dtype())
    df.to_csv(VACCINES_SQUARE_FILE)


BASE_URL = 'https://www.unidoscontraelcovid.gob.bo/index.php/wp-json/wp/v2/posts?categories=50'
TEMPORAL_FILE = '/tmp/temporal.pdf'

VACCINES_FILE = './processed/bolivia/vaccinations.flat.csv'
VACCINES_SQUARE_FILE = './processed/bolivia/vaccinations.csv'
CASES_FILE = './processed/bolivia/cases.flat.csv'

TIMEOUT = 180
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64)'
}

if __name__ == '__main__':
    cdata = requests.get(BASE_URL, headers=HEADERS, timeout=TIMEOUT)
    latest_posts = cdata.json()

    for latest_post in latest_posts[:4][::-1]:
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

        try:
            post_attachment = next(_ for _ in post_attachment if _.endswith('pdf'))
        except StopIteration:
            print('Err: No PDF!')
            continue

        post_attachment_name = os.path.basename(post_attachment)
        post_attachment = requests.get(
            post_attachment, headers=HEADERS, timeout=TIMEOUT
        )

        with open(TEMPORAL_FILE, 'wb') as f:
            f.write(post_attachment.content)

        post_date = post_title.rsplit(' ', 1)[1]
        post_date = pd.to_datetime(post_date, dayfirst=True)

        try:
            post_date_file_name = post_attachment_name.rsplit('.', 1)[0]
            post_date_file_name = post_date_file_name.rsplit('-', 1)[1]

            post_date_file_name = pd.to_datetime(
                post_date_file_name, format='%d_%m_%Y'
            )

            if (
                post_date_file_name > post_date and
                post_date_file_name < pd.to_datetime('today')
            ):
                post_date = post_date_file_name

        except:
            pass


        if (
            'casos' in post_title or
            post_attachment_name.lower().startswith('reporte-nacional-')
        ):
            do_update(
                parse_cases,
                post_date,
                CASES_FILE
            )

        elif (
            'vacunacion' in post_title or
            post_attachment_name.startswith('reporte-de-vacunas-')
        ):
            do_update(
                parse_vaccination,
                post_date,
                VACCINES_FILE
            )
            do_synk_vaccionations()
