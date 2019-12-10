#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019-12-10 16:55
# @Author  : Luke
# @File    : imysql.py
# @Desc    : mysql connection
import connection
import configparser
import os
import pymysql
import utils
import datetime


class MySQLConnection(connection.Connection):
    def __init__(self):
        super().__init__()
        self.connection = self.connect()
        self.user_id = os.getenv("user_id")
        self.workspace_id = os.getenv("workspace_id")
        self.data_table = "zls_test_data"

    def connect(self, **kwargs):
        config = configparser.ConfigParser()
        config.read("idsw-notebook.conf", encoding="utf-8")
        connection = pymysql.connect(host=config.get("mysql", "host"),
                                     port=int(config.get("mysql", "port")),
                                     user=config.get("mysql", "username"),
                                     password=config.get("mysql", "password"),
                                     db=config.get("mysql", "db"), **kwargs)
        return connection

    def disconnect(self):
        self.connection.close()

    def get_root_path(self):
        return "mysql"

    def get_internal_path(self):
        return "mysql"

    def open_file(self, path):
        temp = path.split("/")
        column = "data"
        table_name = self.data_table
        id = temp[1]
        if len(temp) > 2:
            table_name = temp[-3]
            column = temp[-1]
            id = temp[-2]

        return self.query_blob_to_bytes(table_name, id, column)

    def query_blob_to_bytes(self, table_name, id, column):
        query_statement = "SELECT " + column + " FROM " + table_name + " WHERE ID= %s"
        blob = None
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query_statement, [id])
                ret = cursor.fetchone()
                blob = ret[0]
        except Exception as e:
            print(e)
        from io import StringIO
        return StringIO(str(blob, "utf-8"))

    def upload_df(self, df, dataset_name):
        if not dataset_name.endswith(".csv"):
            dataset_name += ".csv"
        tmp_csv_name = utils.generate_uuid() + '.csv'
        df.to_csv(tmp_csv_name, index=False)
        try:
            dataset_size = self._humanbytes(df.memory_usage(index=True).sum())
            self.upload_local_file(tmp_csv_name, dataset_name, dataset_size)
        except Exception as e:
            print(e)
        finally:
            os.remove(tmp_csv_name)

    def upload_local_file(self, csv_file, dataset_name="", dataset_size=""):
        dst = self.get_root_path() + "/" + self.user_id + "/dataset/" + utils.generate_uuid()
        temp = dst.split("/")
        table = self.data_table
        id = temp[-1]
        insert_path = self.insert_file(csv_file, table, id)
        if insert_path is not None:
            resource_id = utils.generate_uuid()
            insert_dataset_statement = """
            INSERT INTO ABC_DATASET (DATASET_ID,WORKSPACE_ID,USER_ID,DATASET_NAME,DATA_DESC,DATASET_TYPE,DATASET_SIZE,DATASET_PATH,IS_ACTIVE,CREATOR,MODIFIER,CREATE_TIME,UPDATE_TIME,START_TIME,END_TIME)
            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            try:
                with self.connection.cursor() as cursor:
                    current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    row_count = cursor.execute(insert_dataset_statement,
                                               [resource_id, self.workspace_id, self.user_id, dataset_name, "", "csv",
                                                dataset_size, insert_path, "1", self.user_id, self.user_id, current_time,
                                                current_time, current_time, current_time])
                self.connection.commit()
                if row_count > 0:
                    insert_resource_dir_statement = """
                    INSERT INTO ABC_RESOURCE_DIR (ITEM_ID,WORKSPACE_ID,ITEM_NAME,ITEM_ORDER,PARENT_ID,ITEM_TYPE,IS_LEAF,RESOURCE_ID,IS_SYS,IS_READONLY,IS_DISPLAY,IS_ACTIVE,CREATOR,MODIFIER,CREATE_TIME,UPDATE_TIME,START_TIME,END_TIME,ITEM_DESC,USER_ID)
                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """
                    try:
                        with self.connection.cursor() as cursor:
                            current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            row_count = cursor.execute(insert_resource_dir_statement,
                                                       [utils.generate_uuid(), self.workspace_id, dataset_name, 1, "-1",
                                                        "1", "1", resource_id, "1", "1", "1", "1", self.user_id,
                                                        self.user_id, current_time, current_time, current_time,
                                                        current_time, "", self.user_id])
                            self.connection.commit()
                    except Exception as e:
                        print(e)
            except Exception as e:
                print(e)

    def insert_file(self, csv_file, table, id):
        insert_statement = "INSERT INTO " + table + " (ID, DATA) values(%s, %s)"
        try:
            with self.connection.cursor() as cursor:
                row_count = cursor.execute(insert_statement, [id, self._convert_to_binary_data(csv_file)])
                self.connection.commit()
                if row_count > 0:
                    record = table + "/" + id
                else:
                    record = None
        except Exception as e:
            print(e)
            record = None
        
        return record

    @staticmethod
    def _convert_to_binary_data(file):
        with open(file, "rb") as f:
            binary_data = f.read()
        return binary_data

