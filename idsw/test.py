#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2019-12-10 15:00
# @Author  : Luke
# @File    : test.py
# @Desc    :
from idsw import dataset
from idsw import model
import os

def test_read_data():
    idataset = dataset.Dataset()
    df = idataset.read_csv("idsw/sysadmin/dataset/64832f7314c64455b1bc3948a94e5fc6")# zls_test_data/69b31a4ab1464b18a3dfa151212f4c49")
    print(df.size)


def test_write_data():
    import pandas as pd
    df = pd.read_csv("/Users/petra/Downloads/Iris.csv", sep=";")
    idataset = dataset.Dataset()
    idataset.save_dataset(df, "test_save2.csv")


def test_write_model():
    from sklearn.linear_model import LogisticRegression
    import pandas as pd
    df = pd.read_csv("/Users/petra/Downloads/Iris.csv", sep=";")
    y = df["label"]
    X = df.drop(["label", "id"], axis=1)
    clf = LogisticRegression(random_state=0).fit(X, y)
    imodel = model.Model()
    imodel.save_model(X, clf, "test_model")
    
def test_load_model():
    import pandas as pd
    df = pd.read_csv("/Users/petra/Downloads/Iris.csv", sep=";")
    X = df.drop(["id", "label"], axis=1)
    imodel = model.Model()
    modela, meta = imodel.load_model("idsw/sysadmin/model/f0261f86903011eaa32cf01898ece9c8")
    if modela is not None:
        print(modela.predict(X))

def test_doc():
    print(help(model))

if __name__ == "__main__":
    os.environ["STORAGE_TYPE"] = "minio"
    os.environ["USER_ID"] = "sysadmin"
    os.environ["WORKSPACE_ID"] = "a"
    os.environ["MINIO_ENDPOINT"] = "10.111.25.27:9000"
    os.environ["MINIO_ACCESSKEY"] = "minioadmin"
    os.environ["MINIO_SECRETKEY"] = "minioadmin"
    os.environ["MINIO_BUCKET"] = "idsw"
    os.environ[
        "MYSQL_CONNECTION"] = "mysql://root:Analysis321!@10.111.24.205:3306/idsw-abc-test"

    # test_read_data()
    # test_write_data()
    # test_write_model()
    test_load_model()
    # test_doc()
