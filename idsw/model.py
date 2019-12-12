#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2019-12-10 16:55
# @Author  : Luke
# @File    : model.py
# @Desc    : model utils

import os
import configparser
from pathlib2 import Path


class Model:
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

    def load_model(self, path):
        if self.connection.closed:
            self.__init__()
        try:
            model, meta = self.connection.open_model(path)
        except Exception as e:
            print("cannot load model")
            model = None
            meta = None
        finally:
            self.connection.disconnect()
        return model, meta

    def save_model(self, df, model, model_name):
        if self.connection.closed:
            self.__init__()
        import pandas as pd
        if not isinstance(df, pd.DataFrame):
            print("Failed! Please provide a pandas DataFrame as the first parameter")
        else:
            try:
                from sklearn.utils.estimator_checks import check_estimator
                check_estimator(model)
                result = self.connection.upload_model(df, model, model_name)
                if result is not None:
                    print("saved successfully")
                self.connection.disconnect()

            except TypeError as e:
                print("Failed! Given model is not a scikit-learn estimator, cannot save")
