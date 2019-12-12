#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019-12-10 16:55
# @Author  : Luke
# @File    : ihdfs.py
# @Desc    : hdfs connection
import os
import hdfs3
from . import utils
from . import connection
import configparser
from pathlib2 import Path
import datetime


class HDFSConnection(connection.Connection):
    def __init__(self):
        super().__init__()
        # self.config = configparser.ConfigParser()
        # self.config.read("idsw-notebook.conf", encoding="utf-8")
        self.connection = self.connect()
        self.user_id = os.getenv("user_id")
        self.workspace_id = os.getenv("workspace_id")

    def connect(self, host=None, port=None, hdfs_ha_enable=None, user=None, kerberos=False):
        if host is None:
            # host = self.config.get("hdfs", "HDFS_HOST")
            host = os.getenv("HDFS_HOST")
        if hdfs_ha_enable is None:
            # hdfs_ha_enable = self.config.getboolean("hdfs", 'HDFS_HA_ENABLE')
            hdfs_ha_enable = True if os.getenv("HDFS_HA_ENABLE").lower() == "true" else False
        if hdfs_ha_enable:
            # kerberos = self.config.get("hdfs", "HDFS_KERBEROS")
            kerberos = True if os.getenv("HDFS_KERBEROS").lower() == "true" else False
            if kerberos:
                connection = hdfs3.HDFileSystem(host=host, pars={"hadoop.security.authentication": "kerberos"})
            else:
                if user is None:
                    # user = self.config.get("hdfs", "HDFS_USER")
                    user = os.getenv("HDFS_USER")
                connection = hdfs3.HDFileSystem(host=host, user=user)
        else:
            if port is None:
                # port = self.config.getint("hdfs", "HDFS_PORT")
                port = int(os.getenv("HDFS_PORT"))
            if kerberos:
                connection = hdfs3.HDFileSystem(host=host, port=port,
                                                     pars={"hadoop.security.authentication": "kerberos"})
            else:
                if user is None:
                    # user = self.config.get("hdfs", "HDFS_USER")
                    user = os.getenv("HDFS_USER")
                connection = hdfs3.HDFileSystem(host=host, port=port, user=user)
        self.closed = False
        return connection
    
    def disconnect(self):
        self.connection.disconnect()
        self.closed = True

    def get_root_path(self):
        # return self.config.get("hdfs", "HDFS_ROOT_PATH")
        if os.getenv("HDFS_ROOT_PATH") is None:
            return "/idsw"
        else:
            return os.getenv("HDFS_ROOT_PATH")

    def get_internal_path(self):
        # return self.config.get("hdfs", "HDFS_ROOT_PATH")
        if os.getenv("HDFS_ROOT_PATH") is None:
            return "/idsw"
        else:
            return os.getenv("HDFS_ROOT_PATH")

    def open_file(self, path):
        from io import StringIO
        with self.connection.open(path) as f:
            data = StringIO(str(f.read(), "utf-8"))
        return data

    def upload_df(self, df, dataset_name):
        if not dataset_name.endswith(".csv"):
            dataset_name += ".csv"
        try:
            dataset_size = self._humanbytes(df.memory_usage(index=True).sum())
            dst = self.get_root_path() + "/" + self.user_id + "/dataset/" + utils.generate_uuid()
            parent_dir = str(Path(dst).parent)
            if not self.connection.exists(parent_dir):
                self.connection.makedirs(parent_dir)
            with self.connection.open(dst, "wb") as writer:
                df.to_csv(writer, index=False, encoding='utf-8')

            from . import imysql
            mysql_connection = imysql.MySQLConnection().connection
            dataset_id = utils.generate_uuid()
            resource_dir_id = utils.generate_uuid()
            insert_dataset_statement = """
                        INSERT INTO ABC_DATASET (DATASET_ID,WORKSPACE_ID,USER_ID,DATASET_NAME,DATA_DESC,DATASET_TYPE,DATASET_SIZE,DATASET_PATH,RESOURCE_DIR_ID,IS_ACTIVE,CREATOR,MODIFIER,CREATE_TIME,UPDATE_TIME,START_TIME,END_TIME)
                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """
            try:
                with mysql_connection.cursor() as cursor:
                    current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    row_count = cursor.execute(insert_dataset_statement,
                                               [dataset_id, self.workspace_id, self.user_id, dataset_name, "", "csv",
                                                dataset_size, dst, resource_dir_id, "1", self.user_id, self.user_id,
                                                current_time,
                                                current_time, current_time, current_time])
                mysql_connection.commit()
                if row_count > 0:
                    insert_resource_dir_statement = """
                                INSERT INTO ABC_RESOURCE_DIR (ITEM_ID,WORKSPACE_ID,ITEM_NAME,ITEM_ORDER,PARENT_ID,ITEM_TYPE,IS_LEAF,RESOURCE_ID,IS_SYS,IS_READONLY,IS_DISPLAY,IS_ACTIVE,CREATOR,MODIFIER,CREATE_TIME,UPDATE_TIME,START_TIME,END_TIME,ITEM_DESC,USER_ID)
                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                """
                    try:
                        with mysql_connection.cursor() as cursor:
                            current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            row_count = cursor.execute(insert_resource_dir_statement,
                                                       [resource_dir_id, self.workspace_id, dataset_name, 1, "-1",
                                                        "1", "1", dataset_id, "1", "1", "1", "1", self.user_id,
                                                        self.user_id, current_time, current_time, current_time,
                                                        current_time, "", self.user_id])
                            mysql_connection.commit()
                            return "success"
                    except Exception as e:
                        print(e)
                        return None
                else:
                    return None
            except Exception as e:
                print(e)
                return None
        except Exception as e:
            print(e)
            return None

    def open_model(self, path):
        model_path = str(Path(path) / "data")
        meta_path = str(Path(path) / "metadata")
        import joblib
        model = None
        meta = None
        with self.connection.open(model_path, "rb") as model_reader:
            try:
                model = joblib.load(model_reader)
            except Exception as e:
                print(e)
        with self.connection.open(meta_path, "rb") as json_reader:
            try:
                import json
                meta = json.load(json_reader)
            except Exception as e:
                print(e)

        return model, meta

    def upload_model(self, df, model, model_name):
        meta = self._form_model_meta(df)
        dst = self.get_root_path() + "/" + self.user_id + "/model/" + utils.generate_uuid()
        if not self.connection.exists(dst):
            self.connection.makedirs(dst)
        import joblib
        import json
        with self.connection.open(dst + "/" + "data", "wb") as writer:
            joblib.dump(model, writer)
        with self.connection.open(dst + "/" + "metadata", "wb") as writer:
            json.dump(meta, writer)

        from . import imysql
        mysql_connection = imysql.MySQLConnection().connection
        model_id = utils.generate_uuid()
        resource_dir_id = utils.generate_uuid()
        insert_model_statement = """
                    INSERT INTO ABC_MODEL (MODEL_ID,WORKSPACE_ID,USER_ID,MODEL_NAME,MODEL_DESC,DEPLOY_ENGINE,MODEL_PATH,MODEL_TYPE,RESOURCE_DIR_ID,IS_ACTIVE,CREATOR,MODIFIER,CREATE_TIME,UPDATE_TIME,START_TIME,END_TIME,MODEL_METADATA)
                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """
        try:
            with mysql_connection.cursor() as cursor:
                current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                row_count = cursor.execute(insert_model_statement,
                                           [model_id, self.workspace_id, self.user_id, model_name, "", "",
                                            dst, "1", resource_dir_id, "1", self.user_id, self.user_id,
                                            current_time, current_time, current_time, current_time, json.dumps(meta)])
            mysql_connection.commit()
            if row_count > 0:
                insert_resource_dir_statement = """
                            INSERT INTO ABC_RESOURCE_DIR (ITEM_ID,WORKSPACE_ID,ITEM_NAME,ITEM_ORDER,PARENT_ID,ITEM_TYPE,IS_LEAF,RESOURCE_ID,IS_SYS,IS_READONLY,IS_DISPLAY,IS_ACTIVE,CREATOR,MODIFIER,CREATE_TIME,UPDATE_TIME,START_TIME,END_TIME,ITEM_DESC,USER_ID)
                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            """
                try:
                    with mysql_connection.cursor() as cursor:
                        current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        row_count = cursor.execute(insert_resource_dir_statement,
                                                   [utils.generate_uuid(), self.workspace_id, model_name, 1, "-1",
                                                    "0", "1", model_id, "1", "1", "1", "1", self.user_id,
                                                    self.user_id, current_time, current_time, current_time,
                                                    current_time, "", self.user_id])
                        mysql_connection.commit()
                        return "sucess"
                except Exception as e:
                    print(e)
                    return None
            else:
                return None
        except Exception as e:
            print(e)
            return None
