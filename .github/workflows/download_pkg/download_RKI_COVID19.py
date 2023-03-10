#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from . import DownloadFile
import os

def download_RKI_COVID19():
    data_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..','..')
    filename = "RKI_COVID19.csv"
    url = "https://media.githubusercontent.com/media/robert-koch-institut/SARS-CoV-2-Infektionen_in_Deutschland/main/Aktuell_Deutschland_SarsCov2_Infektionen.csv"

    a = DownloadFile(url=url, filename=filename, download_path=data_path, compress=True,add_date=True,add_latest=False)
    a.write_file()
