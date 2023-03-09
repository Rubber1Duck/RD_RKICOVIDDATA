#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from . import DownloadFile
import os
from datetime import datetime

def download_RKI_COVID19():
    dateNow = datetime.now().date().isoformat()
    data_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..','..')
    filename = "RKI_COVID19.csv"
    url = "https://github.com/robert-koch-institut/SARS-CoV-2-Infektionen_in_Deutschland_Archiv/raw/master/Archiv/" + dateNow +"_Deutschland_SarsCov2_Infektionen.csv"

    a = DownloadFile(url=url, filename=filename, download_path=data_path, compress=True,add_date=True,add_latest=False)
    a.write_file()
