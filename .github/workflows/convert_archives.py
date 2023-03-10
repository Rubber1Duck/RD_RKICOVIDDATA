import os
import datetime as dt
import pandas as pd
import re


# %%
path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..')
path_fallzahlen = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'Fallzahlen',
                               'RKI_COVID19_Fallzahlen.csv')
pattern = 'RKI_COVID19'
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

CV_dtypes_old = {
    'IdLandkreis': 'Int32',
    'Altersgruppe': 'str',
    'Geschlecht': 'str',
    'NeuerFall': 'Int32',
    'NeuerTodesfall': 'Int32',
    'AnzahlFall': 'Int32',
    'AnzahlTodesfall':'Int32',
    'Meldedatum':'object'}

iso_date_re = '([0-9]{4})(-?)(1[0-2]|0[1-9])\\2(3[01]|0[1-9]|[12][0-9])'
file_list = os.listdir(path)
file_list.sort(reverse=False)
all_files = []
for file in file_list:
    file_path_full = os.path.join(path, file)
    if not os.path.isdir(file_path_full):
        filename = os.path.basename(file)
        re_filename = re.search(pattern, filename)
        re_search = re.search(iso_date_re, filename)
        if re_search and re_filename:
            report_date = dt.date(
                int(re_search.group(1)),
                int(re_search.group(3)),
                int(re_search.group(4))).strftime('%Y-%m-%d')
            all_files.append((file_path_full, report_date))
for file_path_full, report_date in all_files:
  start = dt.datetime.now()
  start.microsecond
  try:
    covid_df = pd.read_csv(file_path_full, usecols=CV_dtypes.keys(), dtype=CV_dtypes)
  except:
    covid_df = pd.read_csv(file_path_full, usecols=CV_dtypes_old.keys(), dtype=CV_dtypes_old)
  try:
    covid_df['Meldedatum'] = pd.to_datetime(covid_df['Meldedatum']).dt.date
  except:
    covid_df['Meldedatum'] = pd.to_datetime(covid_df['Meldedatum'], unit='ms').dt.date
  covid_df.sort_values(by=['IdLandkreis', 'Altersgruppe' ,'Geschlecht', 'Meldedatum'], axis=0, inplace=True, ignore_index=True)
  size_old = os.path.getsize(file_path_full)
  lines = covid_df.shape[0]

  # archiv data File
  dataFile ="RKI_COVID19_" + report_date + ".csv"
  dataFilePath = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', dataFile)
  try:
    with open(dataFilePath, 'wb') as dataCsv:
      covid_df.to_csv(dataCsv, index=False, header=True, lineterminator='\n', encoding= 'utf-8',
                      date_format='%Y-%m-%d', columns=CV_dtypes.keys())
  except:
    with open(dataFilePath, 'wb') as dataCsv:
      covid_df.to_csv(dataCsv, index=False, header=True, lineterminator='\n', encoding= 'utf-8',
                      date_format='%Y-%m-%d', columns=CV_dtypes_old.keys()) 
  size_new = os.path.getsize(dataFilePath)
  end = dt.datetime.now()
  end.microsecond
  print(dataFile,
        ': old size:',
        size_old,
         ', new size: ',
        size_new,
        '=',
        round(size_new/size_old*100,2),
        '% lines:',
        lines,
        'prozessing time:',
        end - start)
