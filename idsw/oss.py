#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2019-06-17 16:53
# @Author  : Luke
# @File    : oss.py
# @Desc    :
import oss2
import uuid
import os
from datetime import datetime
import configparser


class OSS:
    def __init__(self, endpoint=None, access_key_id=None, access_key_secret=None, bucket_name=None):
        config = configparser.ConfigParser()
        config.read("/home/jovyan/work/idsw-notebook.conf", encoding="utf-8")
        init_endpoint = endpoint if endpoint is not None else config.get("oss", "endpoint")
        init_access_key_id = access_key_id if access_key_id is not None else config.get("oss", "access_key_id")
        init_access_key_secret = access_key_secret if access_key_secret is not None else config.get("oss", "access_key_secret")
        init_bucket_name = bucket_name if bucket_name is not None else config.get("oss", "bucket_name")

        self.bucket = oss2.Bucket(oss2.Auth(init_access_key_id, init_access_key_secret), init_endpoint, init_bucket_name)

    @staticmethod
    def _connect_mysql():
        '''
        Using pymysql to connect mysql database
        :return: connection
        '''
        import pymysql
        config = configparser.ConfigParser()
        config.read("idsw-notebook.conf", encoding="utf-8")
        db = pymysql.connect(host=config.get("mysql", "host"),
                             port=int(config.get("mysql", "port")),
                             user=config.get("mysql", "username"),
                             password=config.get("mysql", "password"),
                             db="idsw-dev")
        return db

    def list_bucket(self, limit=None):
        '''
        print file list of the bucket with a limit number or without
        :param limit: int, default None
        :return: None
        examples:
        from idsw import oss
        bucket = oss.OSS().bucket
        bucket.list_bucket()
        # bucket.list_bucket(limit=10)
        '''
        if limit is not None:
            for i, object_info in enumerate(oss2.ObjectIterator(self.bucket)):
                print("{0} {1}".format(object_info.last_modified, object_info.key))

                if i >= int(limit):
                    break
        else:
            for i, object_info in enumerate(oss2.ObjectIterator(self.bucket)):
                print("{0} {1}".format(object_info.last_modified, object_info.key))

    def upload_local_file(self, remote_file, local_file):
        '''
        upload a locol file to oss
        :param remote_file: oss file name
        :param local_file: local file name
        :return: None
        '''
        self.bucket.put_object_from_file(remote_file, local_file)

    def upload_dataframe(self, remote_file, df, user=None):
        '''
        upload a DataFrame object as a csv file, then add a record to IDSW database
        :param remote_file: oss file name
        :param df: pandas.DataFrame
        :param user: current user name
        :return:
        '''
        tmp_csv_name = str(uuid.uuid1()) + '.csv'
        df.to_csv(tmp_csv_name, index=False)
        try:
            self.upload_local_file(remote_file, tmp_csv_name)
        except Exception as e:
            print(e)
            os.remove(tmp_csv_name)
        data_id = str(uuid.uuid1()).replace("-", "")
        data_name = remote_file
        data_path = remote_file
        data_type = "data"
        data_size = self._humanbytes(os.path.getsize(tmp_csv_name))
        user_id = user if user is not None else 'superadmin'
        create_time = datetime.fromtimestamp(os.path.getctime(tmp_csv_name)).strftime('%Y-%m-%d %H:%M:%S')

        db = self._connect_mysql()
        cursor = db.cursor()
        insert_statement = """
        INSERT INTO dsw_dataset (data_id, data_name, data_path, data_type, data_size, user_id, create_time) values(%s, %s, %s, %s, %s, %s, %s)
        """
        try:
            cursor.execute(insert_statement, [data_id, data_name, data_path, data_type, data_size, user_id, create_time])
            cursor.commit()
        except Exception as e:
            print(e)
        finally:
            cursor.close()
            db.close()
            os.remove(tmp_csv_name)

    def upload_sklearn_model(self, remote_file, model, user=None):
        '''
        upload a sklearn model as a pkl file, then add a record to IDSW database
        :param remote_file:
        :param model:
        :param user:
        :return:
        '''
        tmp_model_name = str(uuid.uuid1()) + '.pkl'
        from sklearn.externals import joblib
        joblib.dump(model, tmp_model_name)
        try:
            self.upload_local_file(remote_file, tmp_model_name)
        except Exception as e:
            print(e)
            os.remove(tmp_model_name)
        id = str(uuid.uuid1()).replace("-", "")
        name = remote_file
        path = remote_file
        user_id = user if user is not None else 'superadmin'
        create_time = datetime.fromtimestamp(os.path.getctime(tmp_model_name)).strftime('%Y-%m-%d %H:%M:%S')

        db = self._connect_mysql()
        cursor = db.cursor()
        insert_statement = """
        INSERT INTO dsw_model (id, name, path, user_id, create_time) values(%s, %s, %s, %s, %s, %s, %s)
        """
        try:
            cursor.execute(insert_statement, [id, name, path, user_id, create_time])
            cursor.commit()
        except Exception as e:
            print(e)
        finally:
            cursor.close()
            db.close()
            os.remove(tmp_model_name)

    def download(self, remote_file, local_file):
        '''
        download a file from oss to local storage
        :param remote_file: oss file name
        :param local_file: local file name
        :return:
        '''
        try:
            self.bucket.get_object_to_file(remote_file, local_file)
        except oss2.exceptions.NoSuchKey as e:
            print(e.request_id)

    def _humanbytes(self, B):
        'Return the given bytes as a human friendly KB, MB, GB, or TB string'
        B = float(B)
        KB = float(1024)
        MB = float(KB ** 2)  # 1,048,576
        GB = float(KB ** 3)  # 1,073,741,824
        TB = float(KB ** 4)  # 1,099,511,627,776

        if B < KB:
            return '{0} {1}'.format(B, 'Bytes' if 0 == B > 1 else 'Byte')
        elif KB <= B < MB:
            return '{0:.2f}K'.format(B / KB)
        elif MB <= B < GB:
            return '{0:.2f}M'.format(B / MB)
        elif GB <= B < TB:
            return '{0:.2f}G'.format(B / GB)
        elif TB <= B:
            return '{0:.2f}T'.format(B / TB)
