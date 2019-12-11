#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2019-12-10 15:00
# @Author  : Luke
# @File    : test.py
# @Desc    :
from idsw import dataset
from idsw import model
import os
os.environ["storage_type"] = "hdfs"
os.environ["user_id"] = "superadmin"
os.environ["workspace_id"] = "a"

def test_read_data():
    idataset = dataset.Dataset()
    df = idataset.read_csv("/data/rapidminerTest/crf_train.csv")# zls_test_data/69b31a4ab1464b18a3dfa151212f4c49")
    print(df.size)


def test_write_data():
    import pandas as pd
    df = pd.read_csv("/Users/petra/Downloads/crf_train.csv")
    idataset = dataset.Dataset()
    idataset.save_dataset(df, "test_save2.csv")


def test_write_model():
    from sklearn.linear_model import LogisticRegression
    import pandas as pd
    df = pd.read_csv("/Users/petra/Downloads/data/Iris.csv")
    X = df.drop("species", axis=1)
    y = df["species"]
    clf = LogisticRegression(random_state=0).fit(X, y)
    imodel = model.Model()
    imodel.save_model(X, clf, "test_model")
    
def test_load_model():
    import pandas as pd
    df = pd.read_csv("/Users/petra/Downloads/data/Iris.csv")
    X = df.drop("species", axis=1)
    imodel = model.Model()
    modela, meta = imodel.load_model("/idsw/superadmin/model/a0803f8c1bbb11eaaaddf01898ece9c8")
    if modela is not None:
        print(modela.predict(X))


if __name__ == "__main__":
    # test_read_data()
    # test_write_data()
    # test_write_model()
    test_load_model()