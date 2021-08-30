#!/usr/bin/env python3
# coding: utf-8

import os
import io
import sys
import shutil
import urllib3
import requests
import warnings
import traceback

import pantab
import unidecode

import aghast
import hist

import update_utils

from bs4 import BeautifulSoup
from zipfile import ZipFile

import numpy as np
import pandas as pd
import boost_histogram as bh


warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)
warnings.filterwarnings('ignore', category=urllib3.exceptions.InsecureRequestWarning)


# base

BASE_DIR = './data/'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64)'
}

MIN_DATE = pd.to_datetime('2020-01-01')
MAX_DATE = pd.to_datetime('today') + pd.Timedelta(days=7)
MAX_DATE = MAX_DATE.round('D')


################################################################################
# Utils
################################################################################


def format_column(column_name):
    column_name = column_name.replace(' de ', ' ')
    column_name = column_name.replace(' del ', ' ')
    column_name = '_'.join(column_name.split(' '))
    column_name = unidecode.unidecode(column_name)

    return column_name.lower()


def parse_date(date, **kwargs):
    date = pd.to_datetime(date, errors='coerce', **kwargs)
    date = date.dt.tz_localize(None).copy()

    date[date < MIN_DATE] = pd.NaT
    date[date > MAX_DATE] = pd.NaT

    return date


def get_iso3166(adm1_df, iso):
    global iso_geo_names, geo_names

    adm1_index = map(lambda _: unidecode.unidecode(_.lower()), adm1_df)
    adm1_index = map(
        lambda _: update_utils.RE_PREFIX.sub('', _).strip(), adm1_index
    )
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


def get_age_histo(df):
    h_hist = hist.Hist(
        hist.axis.Integer(start=0, stop=120, flow=True),
        storage=bh.storage.Int64()
    )

    h_hist.fill(df['edad'])

    return aghast.to_pandas(
        aghast.from_numpy(h_hist.to_numpy(flow=True))
    )


BASE_COLUMNS = ['adm1_isocode', 'sex', 'date', 'bin']
def storage_format(df, iso_code, has_iso_name=True):
    fields = [_ for _ in df.columns if _ != 'unweighted']
    df['iso_code'] = iso_code

    df.index.names = BASE_COLUMNS
    df = df.reset_index()

    df['bin'] = df['bin'].map(lambda _: _.left).astype(int)

    if has_iso_name:
        adm1_df = get_iso3166(df['adm1_isocode'].unique(), iso_code)
        df['adm1_isocode'] = df['adm1_isocode'].map(adm1_df['geocode'].to_dict())

    df = df.set_index(
        fields + ['iso_code'] + BASE_COLUMNS
    )
    df = df['unweighted'].groupby(level=df.index.names).sum()
    df = df.unstack(level='bin')

    return df


def get_age_multi_histo(
    df, iso_code, case_states, grouper, histo_df=None, **kwargs
):
    if histo_df is None:
        histo_df = pd.DataFrame([])

    for state_column, state_key in case_states:
        data_df = df[~df[state_column].isna()]
        grouper_ = [*grouper, pd.Grouper(key=state_column, freq='W')]

        histo_df_ = data_df.groupby(grouper_).apply(get_age_histo)
        histo_df_['state'] = state_key

        histo_df_ = storage_format(histo_df_, iso_code, **kwargs)
        histo_df = pd.concat([histo_df, histo_df_])

    return histo_df


def get_diff_histo(df, since, until):
    h_hist = hist.Hist(
        hist.axis.Integer(start=-30, stop=90, flow=True),
        storage=bh.storage.Int64()
    )

    data = (df[until] - df[since]).dt.days.dropna()
    h_hist.fill(data)

    return aghast.to_pandas(
        aghast.from_numpy(h_hist.to_numpy(flow=True))
    )


def get_diff_multi_histo(
    df, iso_code, case_intervals, case_state, grouper, **kwargs
):
    histo_df = pd.DataFrame([])

    for since, until in case_intervals:
        data_df = df[~df[until].isna()]
        grouper_ = [*grouper, pd.Grouper(key=until, freq='W')]

        histo_df_ = data_df.groupby(grouper_).apply(
            lambda _: get_diff_histo(_, since=since, until=until)
        )
        histo_df_['since'] = case_state[since]
        histo_df_['until'] = case_state[until]

        histo_df_ = storage_format(histo_df_, iso_code, **kwargs)
        histo_df = pd.concat([histo_df, histo_df_])

    return histo_df


################################################################################
# Microdata
################################################################################


# Peru

def download_peru(URL, **kwargs):
    if not URL.startswith('/'):
        cdata = requests.get(URL, headers=HEADERS, timeout=30)
        fd = io.BytesIO(cdata.content)
    else:
        fd = open(URL)

    df = pd.read_csv(fd, sep=';', **kwargs)
    df.columns = [_.lower() for _ in df.columns]

    df = df.drop(columns=[df.columns[0], 'fecha_corte'])
    df['sexo'] = df['sexo'].str.upper().map({
        'MASCULINO': 'M', 'FEMENINO': 'F'
    })
    df = df[~df['sexo'].isna()]

    df_date_columns = [_ for _ in df.columns if _.startswith('fecha_')]
    df[df_date_columns] = df[df_date_columns].apply(
        lambda _: parse_date(_, format='%Y%m%d')
    )

    return df


PERU_BASE_URL = 'https://cloud.minsa.gob.pe/s'
PERU_CASES_URL = PERU_BASE_URL + '/AC2adyLkHCKjmfm/download'
PERU_DEATHS_URL = PERU_BASE_URL + '/xJ2LQ3QyRW38Pe5/download'

CASE_STATE_PE = {
    'fecha_resultado': 'confirmed',
    'fecha_fallecimiento': 'dead',
}
GROUPER_PE = [
    pd.Grouper(key='departamento'),
    pd.Grouper(key='sexo'),
]

def update_peru():
    # Casos
    peru_df = download_peru(PERU_CASES_URL, encoding='utf-8')
    peru_df = peru_df.sort_values('fecha_resultado')
    peru_df['departamento'] = peru_df['departamento'].replace({
        'LIMA', 'LIMA METROPOLITANA'
    })

    histo_age_df = get_age_multi_histo(
        peru_df, 'PE', [('fecha_resultado', 'confirmed')], GROUPER_PE
    )

    # Fallecidos
    peru_deaths_df = download_peru(PERU_DEATHS_URL, encoding='utf-8')
    peru_deaths_df = peru_deaths_df.sort_values('fecha_fallecimiento')
    peru_deaths_df = peru_deaths_df.rename(columns={
        'edad_declarada': 'edad'
    })

    histo_age_df = get_age_multi_histo(
        peru_deaths_df,
        'PE',
        [('fecha_fallecimiento', 'dead')],
        GROUPER_PE,
        histo_age_df
    )

    peru_deaths_df = pd.merge(
        peru_deaths_df.dropna(),
        peru_df.dropna(),
        on='id_persona'
    )
    peru_deaths_df = peru_deaths_df[
        peru_deaths_df['edad_x'] == peru_deaths_df['edad_y']
    ]
    peru_deaths_df = peru_deaths_df.rename(columns={
        'departamento_x': 'departamento',
        'sexo_x': 'sexo'
    })

    CASE_KEYS = list(CASE_STATE_PE.keys())
    CASE_INTERVALS = list(zip(CASE_KEYS[:-1], CASE_KEYS[1:]))

    histo_diff_df = get_diff_multi_histo(
        peru_deaths_df, 'PE', CASE_INTERVALS, CASE_STATE_PE, GROUPER_PE
    )

    return {'histo_age': histo_age_df, 'histo_diff': histo_diff_df}


# Colombia

COLOMBIA_URL = 'https://www.datos.gov.co/api/views/gt2j-8ykr/rows.csv?accessType=DOWNLOAD'

CASE_STATE_CO = {
    'fecha_inicio_sintomas': 'symptom_onset',
    'fecha_notificacion': 'tested',
    'fecha_diagnostico': 'confirmed',
    'fecha_muerte': 'dead',
}
GROUPER_CO = [
    pd.Grouper(key='nombre_departamento'),
    pd.Grouper(key='sexo'),
]

def update_colombia():
    colombia_df = pd.read_csv(COLOMBIA_URL)
    colombia_df.columns = [format_column(_) for _ in colombia_df.columns]
    colombia_df = colombia_df.drop(columns=[
        'codigo_iso_pais', 'nombre_pais', 'pertenencia_etnica', 'nombre_grupo_etnico'
    ])

    colombia_df_date_columns = colombia_df.columns[
        colombia_df.columns.str.startswith('fecha')
    ]

    colombia_df[colombia_df_date_columns] = colombia_df[colombia_df_date_columns].apply(
        lambda _: parse_date(_, dayfirst=True)
    )

    # Q: ¿Cómo debemos interpretar los casos que tienen asignada fecha de
    # muerte, pero su estado es N/A?
    # R: Los casos con clasificación N/A corresponden a casos que fallecieron
    # pero por otras causas diferentes a Covid-19
    # Fuente: Debatir en Casos-positivos-de-COVID-19-en-Colombia/gt2j-8ykr/data
    colombia_df.loc[
        colombia_df['estado'].isna()&
        ~colombia_df['fecha_muerte'].isna(),
        'fecha_muerte'
    ] = pd.NaT

    colombia_df.loc[colombia_df['unidad_medida_edad'].isin([2, 3]), 'edad'] = 0
    colombia_df['sexo'] = colombia_df['sexo'].str.upper()
    colombia_df['nombre_departamento'] = colombia_df[
        'nombre_departamento'
    ].str.lower().replace({
        'barranquilla': 'atlantico',
        'cartagena': 'bolivar',
        'guajira': 'la guajira',
        'san andres': 'san andres y providencia',
        'sta marta d.e.': 'magdalena',
        'valle': 'valle del cauca',
        'norte santander': 'norte de santander'
    })

    histo_age_df = get_age_multi_histo(
        colombia_df,
        'CO',
        [(_,__) for _, __ in CASE_STATE_CO.items() if __ != 'tested'],
        GROUPER_CO,
    )

    CASE_KEYS = list(CASE_STATE_CO.keys())
    CASE_INTERVALS = list(zip(CASE_KEYS[:-1], CASE_KEYS[1:]))

    histo_diff_df = get_diff_multi_histo(
        colombia_df, 'CO', CASE_INTERVALS, CASE_STATE_CO, GROUPER_CO
    )

    return {'histo_age': histo_age_df, 'histo_diff': histo_diff_df}


# Argentina


ARGENTINA_URL = 'https://sisa.msal.gov.ar/datos/descargas/covid-19/files/Covid19Casos.zip'
def do_download_argentina():
    cdata = requests.get(ARGENTINA_URL, headers=HEADERS, timeout=30)
    zipfile = ZipFile(io.BytesIO(cdata.content))

    for zipfile_path in zipfile.namelist():
        data_file = zipfile.open(zipfile_path)
        file_path = os.path.join('/tmp', zipfile_path)

        with open(file_path, 'wb') as dest_file:
            shutil.copyfileobj(data_file, dest_file)

    return file_path


ARGENTINA_COL = [
    'id_evento_caso', 'sexo', 'edad', 'edad_años_meses', 'carga_provincia_nombre',
    'fecha_inicio_sintomas', 'fecha_apertura', 'fecha_internacion',
    'fecha_cui_intensivo', 'fecha_fallecimiento', 'fecha_diagnostico',
    'clasificacion_resumen'
]
ARGENTINA_TESTING_STATE = {
    'Descartado': 0,
    'Confirmado': 1,
    'Sospechoso': 2,
    'Sin Clasificar': 3
}

CASE_STATE_AR = {
    'fecha_inicio_sintomas': 'symptom_onset',
    'fecha_apertura': 'tested',
    'fecha_diagnostico': 'confirmed',
    'fecha_internacion': 'hospitalized',
    'fecha_cui_intensivo': 'intensive_care',
    'fecha_fallecimiento': 'dead',
}
GROUPER_AR = [
    pd.Grouper(key='carga_provincia_nombre'),
    pd.Grouper(key='sexo'),
]

def download_argentina(_retry=0):
    try:
        file_path = do_download_argentina()
        argentina_df = pd.read_csv(
            file_path,
            index_col='id_evento_caso',
            usecols=ARGENTINA_COL
        )

    except Exception as e:
        if _retry < 3:
            return download_argentina(_retry + 1)
        else:
            raise(e)

    argentina_df_date_columns = argentina_df.columns[
        argentina_df.columns.str.startswith('fecha')
    ]
    argentina_df[argentina_df_date_columns] = argentina_df[
        argentina_df_date_columns
    ].apply(parse_date)

    argentina_df['clasificacion_resumen'] = argentina_df[
        'clasificacion_resumen'
    ].replace(ARGENTINA_TESTING_STATE).astype(int)

    argentina_df.loc[argentina_df['edad_años_meses'] == 'Meses', 'edad'] = 0
    argentina_df['sexo'] = argentina_df['sexo'].replace('NR', 'U')

    argentina_df = argentina_df.drop(columns='edad_años_meses')

    return argentina_df


def update_argentina():
    argentina_df = download_argentina()

    histo_age_df = get_age_multi_histo(
        argentina_df, 'AR', [('fecha_apertura', 'tested')], GROUPER_AR
    )

    argentina_df = argentina_df[
        argentina_df['clasificacion_resumen'] == ARGENTINA_TESTING_STATE['Confirmado']
    ]
    argentina_df = argentina_df.drop(columns='clasificacion_resumen')

    histo_age_df = get_age_multi_histo(
        argentina_df,
        'AR',
        [(_,__) for _, __ in CASE_STATE_AR.items() if __ != 'tested'],
        GROUPER_AR,
        histo_age_df
    )

    CASE_KEYS = list(CASE_STATE_AR.keys())
    CASE_INTERVALS = list(zip(CASE_KEYS[:-1], CASE_KEYS[1:]))
    CASE_INTERVALS.append(('fecha_diagnostico', 'fecha_fallecimiento'))

    histo_diff_df = get_diff_multi_histo(
        argentina_df, 'AR', CASE_INTERVALS, CASE_STATE_AR, GROUPER_AR
    )

    return {'histo_age': histo_age_df, 'histo_diff': histo_diff_df}


# Paraguay

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

GROUPER_PY = [
    pd.Grouper(key='departamento'),
    pd.Grouper(key='sexo'),
]

def update_paraguay():
    req = requests.get(
        PARAGUAY_URL, headers={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64)'}
    )

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

    histo_age_df = get_age_multi_histo(
        cases_df, 'PY', [('fecha_confirmacion', 'confirmed')], GROUPER_PY
    )
    histo_age_df = get_age_multi_histo(
        deaths_df, 'PY', [('fecha_obito', 'dead')], GROUPER_PY, histo_age_df
    )

    return {'histo_age': histo_age_df}


# Brazil

# Hospitalizados
BRASIL_BASE_URL = 'https://opendatasus.saude.gov.br/dataset/'
BRASIL_SRAG_URLS = [
    # BRASIL_BASE_URL + 'bd-srag-2020',
    BRASIL_BASE_URL + 'bd-srag-2021',
]

CASE_STATE_SRAG_BR = {
    'dt_sin_pri': 'symptom_onset',
    'dt_coleta': 'tested',
    'dt_pcr': 'confirmed',
    'dt_interna': 'hospitalized',
    'dt_entuti': 'intensive_care',
    'dt_saiduti': 'intensive_care_discharge',
    'dt_obito': 'dead',
}
GROUPER_BR = [
    pd.Grouper(key='sg_uf'),
    pd.Grouper(key='sexo'),
]

def update_brazil_srag():
    brasil_srag_df = pd.DataFrame([])

    for BRASIL_SRAG_URL in BRASIL_SRAG_URLS:
        req = requests.get(
            BRASIL_SRAG_URL,
            verify=False,
            timeout=30
        )
        html = BeautifulSoup(req.text, 'html.parser')

        for el in html.find_all('li', {'class': 'resource-item'}):
            heading = el.find('a', {'class': 'heading'})
            title = heading.attrs['title']

            if not title.startswith('SRAG'):
                continue

            dataset_date = pd.to_datetime(title[5:], dayfirst=True)
            # TODO: Check date ?

            datase_url = el.find('a', {'class': 'resource-url-analytics'})
            datase_url = datase_url.attrs['href']

            dataset_df = pd.read_csv(
                datase_url,
                error_bad_lines=False,
                warn_bad_lines=False,
                sep=';'
            )
            dataset_df.columns = [format_column(_) for _ in dataset_df.columns]

            dataset_df = dataset_df.drop(columns=[
                'sg_uf_not', 'id_pais', 'id_regiona', 'id_municip',
                'id_unidade', 'id_rg_resi', 'id_mn_resi', 'obes_imc',
                'out_antiv', 'fluasu_out', 'flubli_out', 'classi_out',
                'lo_ps_vgm', 'sg_uf_inte', 'out_anim', 'id_rg_inte',
                'ds_an_out', 'id_mn_inte', 'pais_vgm'
            ])

            # Format date
            dataset_df_date_columns = dataset_df.columns[
                dataset_df.columns.str.startswith('dt_')
            ]
            dataset_df[dataset_df_date_columns] = dataset_df[
                dataset_df_date_columns
            ].apply(
                lambda _: parse_date(_, dayfirst=True)
            )

            # SRAG Covid
            dataset_df = dataset_df[dataset_df['classi_fin'] == 5]
            # clean duplicate indexes
            dataset_df = dataset_df.set_index(
                ['co_regiona', 'co_mun_not', 'co_uni_not'], append=True
            )
            dataset_df = dataset_df[~dataset_df.index.duplicated()]

            brasil_srag_df = pd.concat([brasil_srag_df, dataset_df])

            del(dataset_df)

    brasil_srag_df = brasil_srag_df.sort_values('dt_notific')

    # clean duplicate indexes
    brasil_srag_df = brasil_srag_df[~brasil_srag_df.index.duplicated()]

    # New field death date
    brasil_srag_df['dt_obito'] = brasil_srag_df[
        brasil_srag_df['evolucao'.lower()] == 2
    ]['dt_evoluca']

    # Format sex/age
    brasil_srag_df.loc[
        brasil_srag_df['tp_idade'].isin([1, 2]), 'nu_idade_n'
    ] = 0
    brasil_srag_df = brasil_srag_df[
        brasil_srag_df['cs_sexo'].isin(['M', 'F'])
    ]
    brasil_srag_df = brasil_srag_df.rename(
        columns={'cs_sexo': 'sexo', 'nu_idade_n': 'edad'}
    )

    # format testing date
    brasil_srag_df['dt_coleta'] = brasil_srag_df[
        ['dt_coleta', 'dt_co_sor']
    ].T.fillna(method='bfill').T['dt_coleta']

    # format diagnostic date (pcr, antigen, serologic, image)
    brasil_srag_df['dt_pcr'] = brasil_srag_df[
        ['dt_pcr', 'dt_res_an', 'dt_res', 'dt_tomo', 'dt_raiox']
    ].T.fillna(method='bfill').T['dt_pcr']

    brasil_srag_df = brasil_srag_df.drop(columns=[
        'tp_idade', 'evolucao', 'dt_co_sor',
        'dt_res_an', 'dt_res', 'dt_tomo', 'dt_raiox'
    ])

    brasil_srag_df['sg_uf'] = 'BR-' + brasil_srag_df['sg_uf']

    # Histograms
    histo_age_df = get_age_multi_histo(
        brasil_srag_df,
        'BR',
        [*CASE_STATE_SRAG_BR.items()],
        GROUPER_BR,
        has_iso_name=False
    )
    # bd-srag-2020 data goes ~ 3 months into 2021
    histo_age_df = histo_age_df.xs(
        slice(pd.to_datetime('2021-03-29'), None),
        level='date',
        drop_level=False
    )

    CASE_KEYS = list(CASE_STATE_SRAG_BR.keys())
    CASE_INTERVALS = list(zip(CASE_KEYS[:-1], CASE_KEYS[1:]))[:-1]
    CASE_INTERVALS.append(('dt_entuti', 'dt_obito'))
    CASE_INTERVALS.append(('dt_pcr', 'dt_obito'))

    histo_diff_df = get_diff_multi_histo(
        brasil_srag_df,
        'BR',
        CASE_INTERVALS,
        CASE_STATE_SRAG_BR,
        GROUPER_BR,
        has_iso_name=False
    )
    # bd-srag-2020 data goes ~ 3 months into 2021
    histo_diff_df = histo_diff_df.xs(
        slice(pd.to_datetime('2021-03-29'), None),
        level='date',
        drop_level=False
    )

    return {'histo_age': histo_age_df, 'histo_diff': histo_diff_df}


BRASIL_CASES_URL = BRASIL_BASE_URL + 'casos-nacionais'

CASE_STATE_BR = {
    'dataInicioSintomas': 'symptom_onset',
    'dataNotificacao': 'tested',
    'dataTeste': 'confirmed', # Falta Revisar, casi seguro que no
}

def update_brazil_cases():
    req = requests.get(
        BRASIL_CASES_URL,
        verify=False,
        timeout=30
    )
    html = BeautifulSoup(req.text, 'html.parser')

    # Get state urls
    br_urls = []

    for el in html.find_all('a', {'class': 'resource-url-analytics'}):
        dataset_url = el.attrs['href']
        if not dataset_url.endswith('.csv'):
            continue

        state_name = os.path.basename(dataset_url).rsplit('.', 1)[0]

        if state_name.startswith('dados-'):
            state_name = state_name[6:]
            state_index = 1

        if '-' in state_name:
            state_name, state_index = state_name.split('-', 1)

        br_urls.append({
            'name': state_name,
            'index': state_index,
            'url': dataset_url
        })

    br_urls = pd.DataFrame.from_dict(br_urls)

    # Fetch state data

    histo_age_df = pd.DataFrame([])
    histo_diff_df = pd.DataFrame([])

    # for _, state_data in br_urls.iterrows():
    for _, state_data in br_urls.groupby('name').tail(2).iterrows():
        state_df = pd.read_csv(
            state_data['url'],
            error_bad_lines=False,
            warn_bad_lines=False,
            sep=';',
            encoding='ISO-8859-1',
        )
        state_df['sg_uf'] = 'BR-' + state_data['name'].upper()

        # Format date
        state_df_date_columns = state_df.columns[
            state_df.columns.str.startswith('data')
        ]
        state_df[state_df_date_columns] = state_df[state_df_date_columns].apply(
            parse_date
        )

        # format sex/age
        state_df['sexo'] = state_df['sexo'].replace({
            'Feminino': 'F', 'Masculino': 'M'
        })
        state_df = state_df[state_df['sexo'].isin(['M', 'F'])]
        state_df = state_df.rename(columns={'idade': 'edad'})

        # Histograms
        histo_age_state_df = get_age_multi_histo(
            state_df,
            'BR',
            [('dataNotificacao', 'tested')],
            GROUPER_BR,
            has_iso_name=False
        )

        state_df = state_df[
            (state_df['resultadoTeste'] == 'Positivo')|
            state_df['classificacaoFinal'].str.startswith('Confirmado').fillna(False)
        ]

        histo_age_state_df = get_age_multi_histo(
            state_df,
            'BR',
            [(_,__) for _, __ in CASE_STATE_BR.items() if __ != 'tested'],
            GROUPER_BR,
            histo_age_state_df,
            has_iso_name=False
        )

        CASE_KEYS = list(CASE_STATE_BR.keys())
        CASE_INTERVALS = list(zip(CASE_KEYS[:-1], CASE_KEYS[1:]))

        histo_diff_state_df = get_diff_multi_histo(
            state_df,
            'BR',
            CASE_INTERVALS,
            CASE_STATE_BR,
            GROUPER_BR,
            has_iso_name=False
        )

        histo_age_df = pd.concat([histo_age_df, histo_age_state_df])
        histo_diff_df = pd.concat([histo_diff_df, histo_diff_state_df])

    return {'histo_age': histo_age_df, 'histo_diff': histo_diff_df}


def update_brazil():
    histo_srag_dfs = update_brazil_srag()
    # histo_cases_dfs = update_brazil_cases()

    return histo_srag_dfs


# Update


BASE_PATH = './processed/stats/'
def do_merge(result_dict):
    for key, df in result_dict.items():
        path = key.split('_')
        base_path = os.path.join(BASE_PATH, '/'.join(path[:-1]))

        if not os.path.isdir(base_path):
            os.makedirs(base_path)

        path = os.path.join(base_path, path[-1])
        path = path + '.csv'

        base_df = pd.read_csv(path)

        base_df['date'] = pd.to_datetime(base_df['date'])
        base_df = base_df.set_index(df.index.names)
        base_df.columns = base_df.columns.astype(int)

        df = df.xs(
            slice(pd.to_datetime('2021-01-04'), None),
            level='date',
            drop_level=False
        )
        df = pd.concat([base_df, df])

        df = df[~df.index.duplicated(keep='last')]
        df = df.sort_index()
        df = df.astype(int)

        df.to_csv(path)


UPDATE_FNS = [
    update_argentina,
    update_peru,
    update_colombia,
    update_paraguay,
    update_brazil,
]
UPDATE_FNS = {_.__name__.split('_')[1]:_ for _ in UPDATE_FNS}
if __name__ == '__main__':
    if len(sys.argv) <= 1:
        exit('error argv')

    try:
        print(sys.argv[1])
        iso_level_0, iso_geo_names, geo_names = update_utils.fetch_geocodes()

        if sys.argv[1] in UPDATE_FNS:
            result_dict = UPDATE_FNS[sys.argv[1]]()
            do_merge(result_dict)


    except Exception as e:
        traceback.print_exc()
