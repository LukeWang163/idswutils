#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2019-12-10 15:00
# @Author  : Luke
# @File    : test.py
# @Desc    :
import dataset
import os
os.environ["storage_type"] = "hdfs"
os.environ["user_id"] = "superadmin"
os.environ["workspace_id"] = "a"
# connection = imysql.MySQLConnection()


def test_read():
    idataset = dataset.Dataset()
    df = idataset.read_csv("/data/rapidminerTest/crf_train.csv")# zls_test_data/69b31a4ab1464b18a3dfa151212f4c49")
    print(df.size)


def test_write():
    import pandas as pd
    df = pd.read_csv("/Users/petra/Downloads/crf_train.csv")
    idataset = dataset.Dataset()
    idataset.save_dataset(df, "test_save2.csv")


if __name__ == "__main__":
    # test_read()
    test_write()
