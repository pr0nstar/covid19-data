#!/usr/bin/env python3
# coding: utf-8

import io
import requests
import warnings
import urllib3
import datetime
import unidecode
import traceback

import update_utils

from bs4 import BeautifulSoup

import pandas as pd
import numpy as np

warnings.filterwarnings('ignore', category=urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=pd.errors.PerformanceWarning)


HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko)'
}


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


def get_population():
    geo_sa_df = pd.read_csv('./update/south.america.population.csv')
    geo_sa_df_ = geo_sa_df.groupby([
        'name_0', 'name_1', 'name_2'
    ])['population'].sum()

    # All adm level 2 with population over 100k or biggest for each adm level 1
    sa_cities = pd.DataFrame([
        *geo_sa_df_[geo_sa_df_ > 1e5].index.to_list(),
        *geo_sa_df_.groupby(level=['name_0', 'name_1']).idxmax().values
    ]).drop_duplicates()

    sa_cities.columns = ['name_0', 'name_1', 'name_2']
    sa_cities = sa_cities.sort_values(['name_0', 'name_1', 'name_2'])

    geo_sa_df = geo_sa_df.set_index(['name_0', 'name_1', 'name_2', 'name_3'])

    return geo_sa_df, sa_cities


DF_ADM1_COLS = [
    'iso_code', 'country_name', 'adm1_isocode',
    'adm1_name', 'frequency', 'date', 'deaths'
]
DF_ADM2_COLS = [
    'iso_code', 'country_name', 'adm1_isocode',
    'adm1_name', 'adm2_name', 'frequency',
    'date', 'deaths'
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

    return df


CHILE_URL = 'https://github.com/MinCiencia/Datos-COVID19/blob/master/output/producto32/Defunciones.csv?raw=true'
def update_chile():
    df = pd.read_csv(CHILE_URL)
    df = df.set_index(['Region', 'Codigo region', 'Comuna', 'Codigo comuna'])
    df.columns = pd.to_datetime(df.columns)

    df = df.stack()
    df = df.reset_index()

    df = df[['Region', 'Comuna', 'level_4', 0]]
    df.columns = ['adm1_name', 'adm3_name', 'date', 'deaths']

    df['adm3_name'] = df['adm3_name'].replace({'Coihaique': 'Coyhaique'})
    df = df.sort_index().groupby(['adm1_name', 'adm3_name', 'date']).sum()
    df = df.reset_index()

    global geo_sa_df, sa_cities

    geo_sa_df_ = geo_sa_df.loc['Chile'].reset_index(['name_2'])['name_2']
    geo_sa_df_ = geo_sa_df_.reset_index('name_1', drop=True)
    geo_sa_df_.index = geo_sa_df_.index.map(unidecode.unidecode).str.lower()

    df['adm2_name'] = geo_sa_df_[
        df['adm3_name'].str.lower().apply(unidecode.unidecode)
    ].values

    df_deaths = df[['adm1_name', 'date', 'deaths']]
    df_deaths = df_deaths.groupby(['adm1_name', 'date']).sum()
    df_deaths = df_deaths.sort_index()
    df_deaths = storage_format(
        df_deaths,
        iso_code='CL',
        frequency='daily',
        country_name='Chile'
    )
    df_deaths = df_deaths[DF_ADM1_COLS]

    df_cities = df.drop('adm3_name', axis=1)
    df_cities = df_cities.groupby(['adm1_name', 'adm2_name', 'date']).sum()

    cities = sa_cities[sa_cities['name_0'] == 'Chile']['name_2']
    cities = pd.concat([cities, cities.apply(unidecode.unidecode)]).drop_duplicates()
    df_cities = df_cities.loc[pd.IndexSlice[:, cities], :]

    df_cities = df_cities.sort_index()
    df_cities = storage_format(
        df_cities,
        iso_code='CL',
        frequency='daily',
        country_name='Chile'
    )
    df_cities = df_cities[DF_ADM2_COLS]

    return {
        'south.america.subnational.mortality': df_deaths,
        'south.america.cities.mortality': df_cities
    }


def do_download_brazil(URL, fields):
    df = pd.read_csv(URL)
    df['date'] = pd.to_datetime(df['date'])

    drop_columns = [_ + '_ibge_code' for _ in fields]
    df = df.drop(drop_columns + ['place'], axis=1)

    df = df.groupby(fields + ['date']).sum().sum(axis=1)
    df = df.reset_index()

    return df


BRAZIL_STATES_URL = 'https://raw.githubusercontent.com/datasets-br/state-codes/master/data/br-state-codes.csv'
BRAZIL_URL = 'https://github.com/capyvara/brazil-civil-registry-data/blob/master/civil_registry_covid_states.csv?raw=true'
BRAZIL_CITIES_URL = 'https://github.com/capyvara/brazil-civil-registry-data/blob/master/civil_registry_covid_cities.csv?raw=true'
def update_brazil():
    state_codes = pd.read_csv(BRAZIL_STATES_URL)
    state_codes = state_codes.set_index('subdivision')

    df = do_download_brazil(BRAZIL_URL, ['state'])
    df.columns = ['adm1_name', 'date', 'deaths']

    df['adm1_name'] = df['adm1_name'].map(
        state_codes['name'].to_dict()
    )

    df = df.set_index(['adm1_name', 'date'])
    df = df.sort_index()

    df_deaths = storage_format(
        df,
        iso_code='BR',
        frequency='daily',
        country_name='Brazil'
    )
    df_deaths = df_deaths[DF_ADM1_COLS]

    df = do_download_brazil(BRAZIL_CITIES_URL, ['state', 'city'])
    df.columns = ['adm1_name', 'adm2_name', 'date', 'deaths']

    df['adm1_name'] = df['adm1_name'].map(
        state_codes['name'].to_dict()
    )

    df = df.set_index(['adm1_name', 'adm2_name', 'date'])
    df = df.sort_index()

    df_cities = storage_format(
        df,
        iso_code='BR',
        frequency='daily',
        country_name='Brazil'
    )
    df_cities = df_cities[DF_ADM2_COLS]

    return {
        'south.america.subnational.mortality': df_deaths,
        'south.america.cities.mortality': df_cities
    }



ECUADOR_URL = 'https://github.com/andrab/ecuacovid/raw/master/datos_crudos/defunciones/por_fecha/cantones_por_dia.csv'
def update_ecuador():
    df = pd.read_csv(ECUADOR_URL)
    df = df.set_index(['provincia', 'canton'])

    df = df.drop('Otro', level=0, errors='ignore')

    df = df.drop([
        'lat', 'lng', 'provincia_poblacion', 'canton_poblacion'
    ], axis=1)

    df = df.astype(int).T
    df.index = pd.to_datetime(df.index, dayfirst=True)

    df = df.unstack().to_frame()
    df = df.reset_index()
    df.columns = ['adm1_name', 'adm2_name', 'date', 'deaths']

    # Patch name format
    df['adm1_name'] = df['adm1_name'].str.replace(
        'Sto. Domingo Tsáchilas', 'Santo Domingo de los Tsáchilas'
    )

    df_deaths = df.groupby(['adm1_name', 'date']).sum()
    df_deaths = df_deaths.sort_index()
    df_deaths = storage_format(
        df_deaths,
        iso_code='EC',
        frequency='daily',
        country_name='Ecuador'
    )
    df_deaths = df_deaths[DF_ADM1_COLS]

    global sa_cities

    cities = sa_cities[sa_cities['name_0'] == 'Ecuador']['name_2']
    cities = pd.concat([cities, cities.apply(unidecode.unidecode)]).drop_duplicates()

    df_cities = df.set_index(['adm1_name', 'adm2_name'])
    df_cities = df_cities.loc[pd.IndexSlice[:, cities], :]

    df_cities = df_cities.set_index('date', append=True)
    df_cities = df_cities.sort_index()
    df_cities = storage_format(
        df_cities,
        iso_code='EC',
        frequency='daily',
        country_name='Ecuador'
    )
    df_cities = df_cities[DF_ADM2_COLS]

    return {
        'south.america.subnational.mortality': df_deaths,
        'south.america.cities.mortality': df_cities
    }


BASE_COLOMBIA_URL = 'https://www.dane.gov.co'
COLOMBIA_URL = BASE_COLOMBIA_URL + '/index.php/estadisticas-por-tema/demografia-y-poblacion/informe-de-seguimiento-defunciones-por-covid-19'
def update_colombia():
    cdata = requests.get(COLOMBIA_URL, verify=False, headers=HEADERS)
    cdata = BeautifulSoup(cdata.text, 'html.parser')

    cdata_docs = cdata.findChild('div', {'class': 'docs-tecnicos'})
    cdata_btns = cdata_docs.find_all('tr')

    download_url = next(
        _ for _ in cdata_btns if 'departamento y sexo' in _.text
    ).findChild('a').attrs['href']

    if download_url.startswith('/'):
        download_url = BASE_COLOMBIA_URL + download_url

    cdata = requests.get(download_url, verify=False, headers=HEADERS)

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

    return {
        'south.america.subnational.mortality': df,
    }


PERU_URL = 'https://cloud.minsa.gob.pe/s/nqF2irNbFomCLaa/download'
def update_peru():
    cdata = requests.get(PERU_URL, headers=HEADERS)
    df = pd.read_csv(
        io.BytesIO(cdata.content),
        delimiter='|',
        encoding='utf-8'
    )

    df['FECHA'] = pd.to_datetime(df['FECHA'])
    df = df.sort_values('FECHA')

    df = df[df['PAIS DOMICILIO'] == 'PERU']

    df['DEPARTAMENTO DOMICILIO'] = df['DEPARTAMENTO DOMICILIO'].str.strip()
    df = df[df['DEPARTAMENTO DOMICILIO'].astype(bool)]
    df['PROVINCIA DOMICILIO'] = df['PROVINCIA DOMICILIO'].str.strip()
    df = df[df['PROVINCIA DOMICILIO'].astype(bool)]

    df = df.groupby([
        'DEPARTAMENTO DOMICILIO', 'PROVINCIA DOMICILIO', 'FECHA'
    ])[df.columns[0]].count().reset_index()
    df.columns = ['adm1_name', 'adm2_name', 'date', 'deaths']

    df_deaths = df.groupby(['adm1_name', 'date'])['deaths'].sum()
    df_deaths = df_deaths.sort_index()

    # Patch Drop Locations: EXTRANJERO/SIN REGISTRO
    df_deaths = df_deaths.drop('EXTRANJERO', level=0, errors='ignore')
    df_deaths = df_deaths.drop('SIN REGISTRO', level=0, errors='ignore')

    df_deaths = storage_format(
        df_deaths,
        iso_code='PE',
        frequency='daily',
        country_name='Peru'
    )
    df_deaths = df_deaths[DF_ADM1_COLS]

    global sa_cities
    cities = sa_cities[sa_cities['name_0'] == 'Peru']['name_2']
    cities = pd.concat([cities, cities.apply(unidecode.unidecode)]).drop_duplicates()

    df['adm2_name'] = df['adm2_name'].str.lower().str.title()

    df_cities = df.set_index(['adm1_name', 'adm2_name'])
    df_cities = df_cities.loc[pd.IndexSlice[:, cities], :]

    df_cities = df_cities.set_index('date', append=True)
    df_cities = df_cities.sort_index()
    df_cities = storage_format(
        df_cities,
        iso_code='PE',
        frequency='daily',
        country_name='Peru'
    )
    df_cities = df_cities[DF_ADM2_COLS]

    return {
        'south.america.subnational.mortality': df_deaths,
        'south.america.cities.mortality': df_cities
    }


PARAGUAY_DEPTS = {
  '01': 'Concepción',
  '02': 'San Pedro',
  '03': 'Cordillera',
  '04': 'Guairá',
  '05': 'Caaguazú',
  '06': 'Caazapá',
  '07': 'Itapúa',
  '08': 'Misiones',
  '09': 'Paraguarí',
  '10': 'Alto Paraná',
  '11': 'Central',
  '12': 'Ñeembucú',
  '13': 'Amambay',
  '14': 'Canindeyú',
  '15': 'Presidente Hayes',
  '16': 'Boquerón',
  '17': 'Alto Paraguay',
  '18': 'Asunción'
}
PARAGUAY_URL = 'http://ssiev.mspbs.gov.py/20170426/defuncion_reportes/lista_multireporte_defuncion.php'
PARAGUAY_DATA = {
    'elegido': 2,
    'xfila': 'coddist',
    'xcolumna': 'EXTRACT(MONTH FROM  fechadef)',
    'anio1': 2021,
    'anio2': 2021,
    'coddpto': None
}
def do_download_paraguay(dept_code, year=2021):
    data = {
        **PARAGUAY_DATA,
        'anio1': year,
        'anio2': year,
        'coddpto': dept_code
    }
    cdata = requests.post(PARAGUAY_URL, data=data)

    df = pd.read_html(
        io.BytesIO(cdata.content), flavor='html5lib', encoding='utf-8'
    )[0]
    df = df.drop(0)

    # Parse HTML format

    df.columns = df.iloc[0]
    df = df.iloc[1:]

    df = df.set_index('Lugar de Defunción/Distrito')
    df = df.drop(['Total', 'EXTRANJERO'], errors='ignore')

    df = df.iloc[:, :-1]

    df = df.applymap(lambda _: int(str(_).replace('.', '')))
    df = df[df.columns[df.sum() > 0]]

    df = df.unstack().reset_index()
    df.columns = ['month', 'lugar', 'deaths']

    df['year'] = year
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

    df.columns = ['adm2_name', 'date', 'deaths']
    df['adm2_name'] = df['adm2_name'].str.lower().str.title()
    df['adm2_name'] = df['adm2_name'].str.replace(
        ' De ', ' de '
    ).str.replace(
        ' Del ', ' del '
    ).str.replace(
        ' El ', ' el ',
    ).str.replace(
        ' La ', ' la ',
    )

    return df


def update_paraguay():
    df = pd.DataFrame([])

    for dept_code, adm1_name in PARAGUAY_DEPTS.items():
        dept_df = do_download_paraguay(dept_code, year=2021)
        dept_df['adm1_name'] = adm1_name

        df = pd.concat([df, dept_df])

    df['adm2_name'] = df['adm2_name'].replace({
        'Mariscal Estigarribia': 'Mariscal Jose Felix Estigarribia'
    })
    df = df[np.roll(df.columns, 1)]


    df_deaths = df.groupby(['adm1_name', 'date']).sum()
    df_deaths = df_deaths.sort_index()
    df_deaths = storage_format(
        df_deaths,
        iso_code='PY',
        frequency='monthly',
        country_name='Paraguay'
    )
    df_deaths = df_deaths[DF_ADM1_COLS]

    global sa_cities
    cities = sa_cities[sa_cities['name_0'] == 'Paraguay']['name_2']
    cities = pd.concat([cities, cities.apply(unidecode.unidecode)]).drop_duplicates()

    df_cities = df.set_index(['adm1_name', 'adm2_name'])
    df_cities = df_cities.loc[pd.IndexSlice[:, cities], :]

    df_cities = df_cities.set_index('date', append=True)
    df_cities = df_cities.sort_index()
    df_cities = storage_format(
        df_cities,
        iso_code='PY',
        frequency='monthly',
        country_name='Paraguay'
    )
    df_cities = df_cities[DF_ADM2_COLS]

    return {
        'south.america.subnational.mortality': df_deaths,
        'south.america.cities.mortality': df_cities
    }


ARGENTINA_URL = 'https://raw.githubusercontent.com/akarlinsky/world_mortality/main/local_mortality/local_mortality.csv'
def update_argentina():
    pass


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

    return {
        'south.america.subnational.mortality': df,
    }


def do_update(fn):
    print(fn.__name__)

    try:
        df_objs = fn()
    except Exception as e:
        traceback.print_exc()
        df_objs = {}

    # >= 2021
    for key, df in df_objs.items():
        df = df[df['date'] > '2020-12-31'].copy()

        df['deaths'] = df['deaths'].astype(int)
        df['date'] = pd.to_datetime(df['date'])

        df_objs[key] = df

    return df_objs


STORAGE_FILE = './raw/mortality/{}.csv'
DF_NON_INDEX_COLS = ['country_name', 'adm1_isocode', 'frequency', 'deaths']
def do_merge(df, path):
    file_name = STORAGE_FILE.format(path)
    base_df = pd.read_csv(file_name)

    order_cols = base_df.columns
    index_cols = [_ for _ in order_cols if _ not in DF_NON_INDEX_COLS]

    base_df['date'] = pd.to_datetime(base_df['date'])
    base_df = base_df.set_index(index_cols)

    df = df.set_index(index_cols)
    df = pd.concat([base_df, df])

    df = df[~df.index.duplicated(keep='last')]
    df = df.sort_index()

    df = df.reset_index()
    df = df[order_cols]

    df.to_csv(file_name, index=False)


UPDATE_FNS = [
    update_chile,
    update_brazil,
    update_ecuador,
    update_colombia,
    update_peru,
    update_paraguay,
    # update_argentina,
    update_bolivia
]
if __name__ == '__main__':
    iso_level_0, iso_geo_names, geo_names = update_utils.fetch_geocodes()
    geo_sa_df, sa_cities = get_population()
    final_df = {}

    for update_fn in UPDATE_FNS:
        df_objs = do_update(update_fn)

        for key, df in df_objs.items():
            fdf = final_df.get(key, pd.DataFrame([]))
            fdf = pd.concat([fdf, df])
            final_df[key] = fdf

    for key, df in final_df.items():
        do_merge(df, key)
