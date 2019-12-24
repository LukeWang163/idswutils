# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019-12-10 16:55
# @Author  : Luke
# @File    : dataset.py
# @Desc    : dataset utils

import os
import pandas as pd


class Dataset:
    """
    utils for connecting with Dataset module
    与平台数据集模块交互的工具类
    Example:
        >>>import idsw.dataset
        >>>idataset = idsw.dataset.Dataset()
        >>>df = idataset.read_csv({{path}})
    """
    def __init__(self):
        storage_type = os.getenv("STORAGE_TYPE")
        self.connection = None
        if storage_type.lower() == "hdfs":
            from . import ihdfs
            self.connection = ihdfs.HDFSConnection()
        elif storage_type.lower() == "mysql":
            from . import imysql
            self.connection = imysql.MySQLConnection()

    def read_csv(self, path, **kwargs):
        """
        read csv from inner storage
        从数据集模块读取csv类型数据集
        Example:
            >>>import idsw.dataset
            >>>idataset = idsw.dataset.Dataset()
            >>>df = idataset.read_csv({{path}})
        @param path: path copied from Dataset module
        @return: Pandas.DataFrame or None
        """
        if self.connection.closed:
            self.__init__()
        df = pd.read_csv(self.connection.open_file(path), encoding='utf-8', **kwargs)
        if df is not None:
            print(df.head())
            self.connection.disconnect()
            return df
        else:
            print("error reading dataset")
            self.connection.disconnect()
            return None

    def read_tsv(self, path, **kwargs):
        """
        read tsv from inner storage
        从数据集模块读取tsv类型数据集
        Example:
            >>>import idsw.dataset
            >>>idataset = idsw.dataset.Dataset()
            >>>df = idataset.read_tsv({{path}})
        @param path: path copied from Dataset module
        @return: Pandas.DataFrame or None
        """
        if self.connection.closed:
            self.__init__()
        df = pd.read_csv(self.connection.open_file(path), sep="\t", encoding='utf-8', **kwargs)
        if df is not None:
            print(df.head())
            self.connection.disconnect()
            return df
        else:
            print("error reading dataset")
            self.connection.disconnect()
            return None

    def read_excel(self, path, **kwargs):
        """
        read excel from inner storage
        从数据集模块读取csv类型数据集
        Example:
            >>>import idsw.dataset
            >>>idataset = idsw.dataset.Dataset()
            >>>df = idataset.read_excel({{path}})
        @param path: path copied from Dataset module
        @return: Pandas.DataFrame or None
        """
        if self.connection.closed:
            self.__init__()
        df = pd.read_excel(self.connection.open_file(path), encoding='utf-8', **kwargs)
        if df is not None:
            print(df.head())
            self.connection.disconnect()
            return df
        else:
            print("error reading dataset")
            self.connection.disconnect()
            return None

    def save_dataset(self, df, dataset_name, dataset_desc=""):
        """
        save Pandas.DataFrame to inner storage
        将Pandas.DataFrame以csv格式存储到数据集模块
        Example:
            >>>import idsw.dataset
            >>>import pandas as pd
            >>>idataset = idsw.dataset.Dataset()
            >>>df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
            >>>idataset.save_dataset(df, "sample_dataset", "sample_dataset_description")
        @param df: Pandas.DataFrame dataframe to save
        @param dataset_name: name to show in Dataset module
        @param dataset_desc: description to show in Dataset module
        """
        if self.connection.closed:
            self.__init__()
        result = self.connection.upload_df(df, dataset_name, dataset_desc)
        if result is not None:
            print("saved successfully")
        else:
            print("encountered error")
        self.connection.disconnect()
