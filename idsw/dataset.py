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
        # config = configparser.ConfigParser()
        # config.read(Path(os.path.realpath(__file__)).parent / "idsw-notebook.conf", encoding="utf-8")
        storage_type = os.getenv("STORAGE_TYPE")
        self.connection = None
        if storage_type.lower() == "hdfs":
            from . import ihdfs
            self.connection = ihdfs.HDFSConnection()
        elif storage_type.lower() == "mysql":
            from . import imysql
            self.connection = imysql.MySQLConnection()

    def read_csv(self, path):
        if self.connection.closed:
            self.__init__()
        df = pd.read_csv(self.connection.open_file(path), encoding='utf-8')
        if df is not None:
            print(df.head())
            self.connection.disconnect()
            return df
        else:
            print("error reading dataset")
            self.connection.disconnect()
            return None

    def read_tsv(self, path):
        if self.connection.closed:
            self.__init__()
        df = pd.read_csv(self.connection.open_file(path), sep="\t", encoding='utf-8')
        if df is not None:
            print(df.head())
            self.connection.disconnect()
            return df
        else:
            print("error reading dataset")
            self.connection.disconnect()
            return None

    def read_excel(self, path):
        if self.connection.closed:
            self.__init__()
        df = pd.read_excel(self.connection.open_file(path), encoding='utf-8')
        if df is not None:
            print(df.head())
            self.connection.disconnect()
            return df
        else:
            print("error reading dataset")
            self.connection.disconnect()
            return None

    def save_dataset(self, df, dataset_name):
        if self.connection.closed:
            self.__init__()
        result = self.connection.upload_df(df, dataset_name)
        if result is not None:
            print("saved successfully")
        else:
            print("encountered error")
        self.connection.disconnect()
        # return result
