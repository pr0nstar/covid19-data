#!/usr/bin/env python3
# coding: utf-8

import io
import json
import requests
import warnings
import urllib3
import datetime
import unidecode

from bs4 import BeautifulSoup

import pandas as pd

warnings.filterwarnings('ignore', category=urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore', category=FutureWarning)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64)'
}

PREFIX = [
    'departamento', 'departament',
    'provincia', 'province',
    'estado', 'state',
    'region'
]
RE_PREFIX = r'\b(?:{})\b'.format('|'.join(PREFIX))
GEO_URL = 'https://raw.githubusercontent.com/esosedi/3166/master/data/iso3166-2.json'
def fetch_geocodes():
    geo_data = requests.get(GEO_URL)
    geo_data = json.loads(geo_data.content)

    # Patch Ñuble
    geo_data['CL']['regions'].append(
        {'name': 'Ñuble Region', 'iso': 'NB', 'names': {'geonames': 'Ñuble'}}
    )

    iso_geo_names = pd.DataFrame([])

    for geo_key in geo_data.keys():
        geo_names = {
            '{}-{}'.format(geo_key, _['iso']): [*_['names'].values()] for _ in geo_data[geo_key]['regions']
        }
        geo_names = pd.DataFrame.from_dict(geo_names, orient='index')
        geo_names = geo_names.fillna('')

        iso_geo_names = pd.concat([iso_geo_names, geo_names])

    iso_geo_names = iso_geo_names.fillna('')
    geo_names = iso_geo_names.stack().droplevel(1).reset_index()

    geo_names.columns = ['geocode', 'name']
    geo_names['name'] = geo_names['name'].map(
        unidecode.unidecode
    ).str.lower().str.replace(
        RE_PREFIX, ''
    ).str.replace(
        r'^ *(de|del) ', ''
    ).str.strip()

    geo_names = geo_names[geo_names['name'] != ''].set_index('name')

    return iso_geo_names, geo_names


def get_iso3166(adm1_df, iso):
    global iso_geo_names, geo_names

    adm1_index = map(lambda _: unidecode.unidecode(_.lower()), adm1_df)
    adm1_index = list(adm1_index)

    country_geo_names = geo_names[geo_names['geocode'].str.startswith(iso)]
    country_geo_names = country_geo_names[
        ~country_geo_names.index.duplicated(keep='first')
    ]

    adm1_index = country_geo_names.loc[adm1_index]

    adm1_index['name'] = iso_geo_names.loc[
        adm1_index['geocode'].values
    ][0].values
    adm1_index.index = adm1_df

    return adm1_index


DF_ORDER_COLS = [
    'iso_code', 'country_name', 'adm1_isocode',
    'adm1_name', 'frequency', 'date', 'deaths'
]
def storage_format(df, iso_code=None, **kwargs):
    df = df.reset_index()
    df['iso_code'] = iso_code

    for k, v in kwargs.items():
        df[k] = v

    adm1_df = df['adm1_name'].unique()
    adm1_df = get_iso3166(adm1_df, iso_code)

    df['adm1_isocode'] = df['adm1_name']
    df['adm1_isocode'] = df['adm1_isocode'].map(
        adm1_df['geocode'].to_dict()
    )

    df['adm1_name'] = df['adm1_name'].map(
        adm1_df['name'].to_dict()
    )

    df['deaths'] = df['deaths'].astype(int)

    df = df[DF_ORDER_COLS]

    return df


CHILE_URL = 'https://github.com/MinCiencia/Datos-COVID19/blob/master/output/producto32/Defunciones.csv?raw=true'
def update_chile():
    df = pd.read_csv(CHILE_URL)
    df = df.set_index(['Region', 'Codigo region', 'Comuna', 'Codigo comuna'])
    df.columns = pd.to_datetime(df.columns)

    df = df.stack()
    df = df.reset_index()

    df = df[['Region', 'level_4', 0]]
    df.columns = ['adm1_name', 'date', 'deaths']

    df = df.sort_index().groupby(['adm1_name', 'date']).sum()
    df = storage_format(
        df,
        iso_code='CL',
        frequency='daily',
        country_name='Chile'
    )

    return df

BRAZIL_STATES_URL = 'https://raw.githubusercontent.com/datasets-br/state-codes/master/data/br-state-codes.csv'
BRAZIL_URL = 'https://github.com/capyvara/brazil-civil-registry-data/blob/master/civil_registry_covid_states.csv?raw=true'
def update_brazil():
    state_codes = pd.read_csv(BRAZIL_STATES_URL)
    state_codes = state_codes.set_index('subdivision')

    df = pd.read_csv(BRAZIL_URL)
    df['date'] = pd.to_datetime(df['date'])

    df = df.drop(['state_ibge_code', 'place'], axis=1)
    df = df.groupby(['state', 'date']).sum().sum(axis=1)

    df = df.reset_index()
    df.columns = ['adm1_name', 'date', 'deaths']

    df['adm1_name'] = df['adm1_name'].map(
        state_codes['name'].to_dict()
    )

    df = df.set_index(['adm1_name', 'date'])
    df = df.sort_index()
    df = storage_format(
        df,
        iso_code='BR',
        frequency='daily',
        country_name='Brazil'
    )

    return df

ECUADOR_URL = 'https://github.com/andrab/ecuacovid/raw/master/datos_crudos/defunciones/por_fecha/provincias_por_dia.csv'
def update_ecuador():
    df = pd.read_csv(ECUADOR_URL)
    df = df.set_index('provincia')

    if 'Otro' in df.index:
        df = df.drop('Otro')

    df = df.drop(['lat', 'lng', 'poblacion'], axis=1)

    df = df.astype(int).T
    df.index = pd.to_datetime(df.index, dayfirst=True)

    df = df.unstack().to_frame()
    df = df.reset_index()
    df.columns = ['adm1_name', 'date', 'deaths']

    # Patch name format
    df['adm1_name'] = df['adm1_name'].str.replace(
        'Sto. Domingo Tsáchilas', 'Santo Domingo de los Tsáchilas'
    )

    df = df.set_index(['adm1_name', 'date'])
    df = df.sort_index()
    df = storage_format(
        df,
        iso_code='EC',
        frequency='daily',
        country_name='Ecuador'
    )

    return df

BASE_COLOMBIA_URL = 'https://www.dane.gov.co'
COLOMBIA_URL = BASE_COLOMBIA_URL + '/index.php/estadisticas-por-tema/demografia-y-poblacion/informe-de-seguimiento-defunciones-por-covid-19'
def update_colombia():
    cdata = requests.get(COLOMBIA_URL, verify=False)
    cdata = BeautifulSoup(cdata.text, 'html.parser')

    cdata_docs = cdata.findChild('div', {'class': 'docs-tecnicos'})
    cdata_btns = cdata_docs.find_all('tr')

    download_url = next(
        _ for _ in cdata_btns if 'departamento y sexo' in _.text
    ).findChild('a').attrs['href']

    if download_url.startswith('/'):
        download_url = BASE_COLOMBIA_URL + download_url

    cdata = requests.get(download_url, verify=False)

    df = pd.read_excel(cdata.content, sheet_name=1, header=None)
    df = df.dropna(how='all').iloc[3:]

    # Parse Excel format

    df.iloc[0] = df.iloc[0].fillna(method='ffill').fillna(
        pd.Series(['anio', 'lugar', 'semana'])
    )
    df.columns = pd.MultiIndex.from_frame(
        df.iloc[:2].T.fillna(method='ffill', axis=1)
    )
    df = df.iloc[2:-4]

    df = df[(df['lugar'] != 'Sin información').to_numpy()]
    df['anio'] = df[(df['lugar'] == 'Total').to_numpy()]['anio'].fillna('2021pr')
    df.iloc[:, :3] = df.iloc[:, :3].fillna(method='ffill')

    df = df[~(df['lugar'] == 'Total').to_numpy()]
    df = df[~(df['semana'] == 'Total').to_numpy()]

    df = df[df.columns[
        df.columns.get_level_values(1) != 'Total'
    ]].reset_index(drop=True)

    df['anio'] = df['anio'].applymap(
        lambda _: _[:-2] if str(_).endswith('pr') else _
    )
    df['anio'] = df['anio'].astype(int)
    df['semana'] = df['semana'].applymap(lambda _: _[7:]).astype(int)

    df.index = pd.MultiIndex.from_frame(df[['anio', 'semana', 'lugar']])
    df = df.iloc[:, 3:]

    df.index.names = [_[0] for _ in df.index.names]
    df.columns.names = ['', '']

    # Format

    df = df.sum(axis=1).reset_index()

    df_index = df[['anio', 'semana']].apply(
        lambda _: '{}-{}-1'.format(_['anio'], _['semana']),
        axis=1
    )
    df_index = df_index.map(
        lambda _: datetime.datetime.strptime(_, "%G-%V-%u")
    )

    df['date'] = df_index
    df = df.drop(['anio', 'semana'], axis=1)

    df.columns = ['adm1_name', 'deaths', 'date']
    df = df.set_index(['adm1_name', 'date'])
    df = df.sort_index()

    # Patch Drop Locations: Extranjero
    df = df.drop('Extranjero', level=0)

    df = storage_format(
        df,
        iso_code='CO',
        frequency='weekly',
        country_name='Colombia'
    )

    return df

PERU_URL = 'https://cloud.minsa.gob.pe/s/nqF2irNbFomCLaa/download'
def update_peru():
    cdata = requests.get(PERU_URL, headers=HEADERS)
    df = pd.read_csv(
        io.BytesIO(cdata.content),
        delimiter=';',
        encoding='unicode_escape'
    )

    df.columns = df.iloc[1]
    df = df.iloc[2:, :-4]

    df['FECHA'] = pd.to_datetime(df['FECHA'])
    df = df.sort_values('FECHA')

    df = df.groupby([
        'DEPARTAMENTO DOMICILIO', 'FECHA'
    ])['Nº'].count().reset_index()

    df.columns = ['adm1_name', 'date', 'deaths']
    df = df.set_index(['adm1_name', 'date'])
    df = df.sort_index()

    # Patch Drop Locations: EXTRANJERO/SIN REGISTRO
    df = df.drop('EXTRANJERO', level=0)
    df = df.drop('SIN REGISTRO', level=0)

    df = storage_format(
        df,
        iso_code='PE',
        frequency='daily',
        country_name='Peru'
    )

    return df

PARAGUAY_URL = 'http://ssiev.mspbs.gov.py/20170426/defuncion_reportes/lista_multireporte_defuncion.php'
def update_paraguay():
    data = {
        'elegido': 2,
        'xfila': 'coddpto',
        'xcolumna': 'EXTRACT(MONTH FROM  fechadef)',
        'anio1': 2021,
        'anio2': 2021
    }
    cdata = requests.post(PARAGUAY_URL, data=data)

    df = pd.read_html(
        io.BytesIO(cdata.content), flavor='html5lib', encoding='utf-8'
    )[0]
    df = df.drop(0)

    # Parse HTML format

    df.columns = df.iloc[0]
    df = df.iloc[1:]

    df = df.set_index('Lugar de Defunción/Dpto.')
    df = df.drop(['Total', 'EXTRANJERO'])
    df = df.iloc[:, :-1]

    df = df.applymap(lambda _: int(str(_).replace('.', '')))
    df = df[df.columns[df.sum() > 0]]

    df = df.unstack().reset_index()
    df.columns = ['month', 'lugar', 'deaths']

    df['year'] = 2021
    df = df[['lugar', 'year', 'month', 'deaths']]

    df['month'] = df['month'].replace({
        'Enero': 1, 'Febrero': 2, 'Marzo': 3,
        'Abril': 4, 'Mayo': 5, 'Junio': 6,
        'Julio': 7, 'Agosto': 8, 'Septiembre': 9, 'Setiembre': 9,
        'Octubre': 10, 'Noviembre': 11, 'Diciembre': 12
    })

    # format

    df['date'] = df[['year', 'month']].apply(
       lambda _: '{}-{}-1'.format(_['year'], _['month']), axis=1
    )
    df['date'] = pd.to_datetime(df['date'])

    df = df.groupby(['lugar', 'date'])['deaths'].sum()
    df = df.reset_index()
    df.columns = ['adm1_name', 'date', 'deaths']

    # Patch name format
    df['adm1_name'] = df['adm1_name'].str.replace(
        'PTE. HAYES', 'PRESIDENTE HAYES'
    ).str.replace(
        'CAPITAL', 'ASUNCION'
    )

    df = df.set_index(['adm1_name', 'date'])
    df = df.sort_index()
    df = storage_format(
        df,
        iso_code='PY',
        frequency='monthly',
        country_name='Paraguay'
    )

    return df

BOLIVIA_URL = 'https://raw.githubusercontent.com/pr0nstar/covid19-data/master/raw/bolivia/sereci/sereci.by.death.date.csv'
def update_bolivia():
    df = pd.read_csv(BOLIVIA_URL, index_col=0)

    df.index = pd.to_datetime(df.index)
    df = df.unstack().reset_index()
    df.columns = ['adm1_name', 'date', 'deaths']

    df = df.set_index(['adm1_name', 'date'])
    df = df.sort_index()
    df = storage_format(
        df,
        iso_code='BO',
        frequency='monthly',
        country_name='Bolivia'
    )

    df['adm1_name'] = df['adm1_name'].str.replace(
        'El Beni', 'Beni'
    )

    return df


def do_update(fn):
    print(fn.__name__)
    try:
        df = fn()
    except Exception as e:
        print(e)
        return

    # >= 2021
    df = df[df['date'] > '2020-12-31']

    df['deaths'] = df['deaths'].astype(int)
    df['date'] = pd.to_datetime(df['date'])

    return df


STORAGE_FILE = './raw/mortality/south.america.subnational.mortality.csv'
DF_INDEX_COLS = ['iso_code', 'adm1_name', 'date']
def do_merge(df):
    base_df = pd.read_csv(STORAGE_FILE)
    base_df['date'] = pd.to_datetime(base_df['date'])
    base_df = base_df.set_index(DF_INDEX_COLS)

    df = df.set_index(DF_INDEX_COLS)

    df = pd.concat([base_df, df])
    df = df[~df.index.duplicated(keep='last')]
    df = df.sort_index()

    df = df.reset_index()
    df = df[DF_ORDER_COLS]

    df.to_csv(STORAGE_FILE, index=False)


UPDATE_FNS = [
    update_chile,
    update_brazil,
    update_ecuador,
    update_colombia,
    update_peru,
    update_paraguay,
    update_bolivia
]
if __name__ == '__main__':
    iso_geo_names, geo_names = fetch_geocodes()
    final_df = pd.DataFrame([])

    for update_fn in UPDATE_FNS:
        final_df = pd.concat([final_df, do_update(update_fn)])

    do_merge(final_df)
