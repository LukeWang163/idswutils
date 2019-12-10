#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2019-12-10 16:55
# @Author  : Luke
# @File    : model.py
# @Desc    : model utils

import os
import configparser


class Model:
    def __init__(self):
        config = configparser.ConfigParser()
        config.read("idsw-notebook.conf", encoding="utf-8")
        storage_type = os.getenv("storage_type")
        self.connection = None
        if storage_type.lower() == "hdfs":
            import ihdfs
            self.connection = ihdfs.HDFSConnection()
        elif storage_type.lower() == "mysql":
            import imysql
            self.connection = imysql.MySQLConnection()

    def load_model(self, path):
        try:
            from sklearn.externals import joblib
            model = joblib.load(self.connection.openModel(path))
        except Exception as e:
            print("cannot load model")
            model = None
        finally:
            self.connection.disconnect()
        return model

    def save_model(self, df, model, model_name):
        try:
            from sklearn.utils import estimator_checks
            estimator_checks.check_estimator(model, generate_only=False)
            self.connection.upload_model(df, model, model_name)
            self.connection.disconnect()
            print("saved successfully")
        except TypeError as e:
            print("given model is not a scikit-learn estimator, cannot save")
