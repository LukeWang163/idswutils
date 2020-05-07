#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2019-12-10 16:55
# @Author  : Luke
# @File    : model.py
# @Desc    : model utils

import os


class Model:
    """
    utils for connecting with Model module
    与平台模型模块交互的工具类
    Example:
        >>>import idsw
        >>>imodel = idsw.model.Model()
        >>>mymodel, model_meta = imodel.load_model("{{path}}")
    """
    def __init__(self):
        storage_type = os.getenv("STORAGE_TYPE")
        self.connection = None
        if storage_type.lower() == "minio":
            from . import iminio
            self.connection = iminio.MinIOConnection()
        elif storage_type.lower() == "mysql":
            from . import imysql
            self.connection = imysql.MySQLConnection()

    def save_model(self, df, model, model_name, model_desc=""):
        """
        save Scikit-Learn model to inner storage and show in Model module
        将Notebook中训练的Scikit-Learn模型保存到模型模块，需提供训练集的Pandas.DataFrame以保存元数据
        Example:
            >>>import idsw
            >>>imodel = idsw.model.Model()
            >>>imodel.save_model(df, cls, "sample_model", "sample_model_description")
        @param df: training DataFrame
        @param model: scikit-learn model
        @param model_name: name to show in Model module
        @param model_desc: model description shown in Model module
        """
        if self.connection.closed:
            self.__init__()
        import pandas as pd
        if not isinstance(df, pd.DataFrame):
            print("Failed! Please provide a pandas DataFrame as the first parameter")
        else:
            try:
                from sklearn.utils.estimator_checks import check_estimator
                check_estimator(model)
                result = self.connection.upload_model(df, model, model_name, model_desc)
                if result is not None:
                    print("saved successfully")
                self.connection.disconnect()

            except TypeError as e:
                print("Failed! Given model is not a scikit-learn estimator, cannot save")

    def load_model(self, path):
        """
        load Scikit-Learn model from inner storage saved from Notebook
        从模型模块加载Scikit-Learn模型
        Example:
            >>>import idsw
            >>>imodel = idsw.model.Model()
            >>>mymodel, model_meta = imodel.load_model("{{path}}")
        @param path: path copied from Model module
        @return: model and model meta
        """
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
