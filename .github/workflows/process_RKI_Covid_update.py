import os, lzma
from datetime import timedelta

import numpy as np
import pandas as pd

from repo_tools_pkg.file_tools import find_latest_file

# %%
path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..')
path_fallzahlen = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'Fallzahlen',
                               'RKI_COVID19_Fallzahlen.csv')

iso_date_re = '([0-9]{4})(-?)(1[0-2]|0[1-9])\\2(3[01]|0[1-9]|[12][0-9])'
pattern = 'RKI_COVID19'
dtypes_fallzahlen = {
    'Datenstand': 'object',
    'IdBundesland': 'Int32',
    'IdLandkreis': 'Int32',
    'AnzahlFall': 'Int32',
    'AnzahlTodesfall': 'Int32',
    'AnzahlFall_neu': 'Int32',
    'AnzahlTodesfall_neu': 'Int32',
    'AnzahlFall_7d': 'Int32',
    'report_date': 'object',
    'meldedatum_max': 'object'}
CV_dtypes = {
    'IdLandkreis': 'Int32',
    'Altersgruppe': 'str',
    'Geschlecht': 'str',
    'NeuerFall': 'Int32',
    'NeuerTodesfall': 'Int32',
    'NeuGenesen': 'Int32',
    'AnzahlFall': 'Int32',
    'AnzahlTodesfall':'Int32',
    'AnzahlGenesen': 'Int32',
    'Meldedatum':'object'}
key_list = ['Datenstand', 'IdBundesland', 'IdLandkreis']


# %% read covid latest
covid_path_latest, date_latest = find_latest_file(os.path.join(path), file_pattern=pattern)
covid_df = pd.read_csv(covid_path_latest, usecols=CV_dtypes.keys(), dtype=CV_dtypes)

# archiv data File
dataFile ="RKI_COVID19_" + date_latest.isoformat() + ".csv.xz"
dataFilePath = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', dataFile)
with lzma.open(dataFilePath, 'wb') as dataCsv:
    covid_df.to_csv(dataCsv, index=False, header=True, lineterminator='\n', encoding= 'utf-8',
                    date_format='%Y-%m-%d', columns=CV_dtypes.keys())

covid_df.insert(loc=0, column='IdBundesland', value=covid_df['IdLandkreis']/1000)
covid_df['IdBundesland'] = covid_df['IdBundesland'].astype(int)
covid_df['Meldedatum'] = pd.to_datetime(covid_df['Meldedatum']).dt.date
covid_df.insert(loc=0, column='Datenstand', value= date_latest)

# %% read fallzahlen current
fallzahlen_df = pd.read_csv(path_fallzahlen, engine='python', dtype=dtypes_fallzahlen, usecols=dtypes_fallzahlen.keys())
fallzahlen_df['Datenstand'] = pd.to_datetime(fallzahlen_df['Datenstand']).dt.date
fallzahlen_df['report_date'] = pd.to_datetime(fallzahlen_df['report_date']).dt.date
fallzahlen_df['meldedatum_max'] = pd.to_datetime(fallzahlen_df['meldedatum_max']).dt.date

# %% eval fallzahlen new
print(date_latest)
covid_df['Meldedatum'] = pd.to_datetime(covid_df['Meldedatum']).dt.date
meldedatum_max = covid_df['Meldedatum'].max()
covid_df['AnzahlFall_neu'] = np.where(covid_df['NeuerFall'].isin([-1, 1]), covid_df['AnzahlFall'], 0)
covid_df['AnzahlFall'] = np.where(covid_df['NeuerFall'].isin([0, 1]), covid_df['AnzahlFall'], 0)
covid_df['AnzahlFall_7d'] = np.where(covid_df['Meldedatum'] > (meldedatum_max - timedelta(days=7)),
                                     covid_df['AnzahlFall'], 0)
covid_df['AnzahlTodesfall_neu'] = np.where(covid_df['NeuerTodesfall'].isin([-1, 1]), covid_df['AnzahlTodesfall'], 0)
covid_df['AnzahlTodesfall'] = np.where(covid_df['NeuerTodesfall'].isin([0, 1]), covid_df['AnzahlTodesfall'], 0)
covid_df.drop(['NeuerFall', 'NeuerTodesfall'], inplace=True, axis=1)
agg_key = {
    c: 'max' if c in ['Meldedatum', 'Datenstand'] else 'sum'
    for c in covid_df.columns
    if c not in key_list
}

covid_df = covid_df.groupby(key_list, as_index=False).agg(agg_key)
covid_df.rename(columns={'Meldedatum': 'meldedatum_max'}, inplace=True)
covid_df['report_date'] = date_latest

# %% concat and dedup
fallzahlen_df = fallzahlen_df[fallzahlen_df['Datenstand'] != date_latest]

fallzahlen_new = pd.concat([fallzahlen_df, covid_df])
fallzahlen_new.drop_duplicates(subset=key_list, keep='last', inplace=True)
fallzahlen_new.sort_values(by=key_list, inplace=True)

# %% write csv
with open(path_fallzahlen, 'wb') as csvfile:
    fallzahlen_new.to_csv(csvfile, index=False, header=True, lineterminator='\n', encoding='utf-8',
                          date_format='%Y-%m-%d', columns=dtypes_fallzahlen.keys())
