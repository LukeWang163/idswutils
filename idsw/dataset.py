# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019-12-10 16:55
# @Author  : Luke
# @File    : dataset.py
# @Desc    : dataset utils

import os
import configparser
import pandas as pd
from pathlib2 import Path


class Dataset:
    def __init__(self):
        config = configparser.ConfigParser()
        config.read(Path(os.path.realpath(__file__)).parent / "idsw-notebook.conf", encoding="utf-8")
        storage_type = os.getenv("storage_type")
        self.connection = None
        if storage_type.lower() == "hdfs":
            from . import ihdfs
            self.connection = ihdfs.HDFSConnection()
        elif storage_type.lower() == "mysql":
            from . import imysql
            self.connection = imysql.MySQLConnection()

    def read_csv(self, path):
        df = pd.read_csv(self.connection.open_file(path), encoding='utf-8')
        self.connection.disconnect()
        print(df.head())
        return df

    def read_tsv(self, path):
        df = pd.read_csv(self.connection.open_file(path), encoding='utf-8')
        self.connection.disconnect()
        print(df.head())
        return df

    def read_excel(self, path):
        df = pd.read_excel(self.connection.open_file(path), encoding='utf-8')
        self.connection.disconnect()
        print(df.head())
        return df

    def save_dataset(self, df, dataset_name):
        self.connection.upload_df(df, dataset_name)
        self.connection.disconnect()
        print("saved successfully")
