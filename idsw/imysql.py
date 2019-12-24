#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019-12-10 16:55
# @Author  : Luke
# @File    : imysql.py
# @Desc    : mysql connection
from . import connection
import os
import pymysql
from . import utils
import datetime


class MySQLConnection(connection.Connection):
    def __init__(self):
        super().__init__()
        self.connection = self.connect()
        self.user_id = os.getenv("USER_ID")
        self.workspace_id = os.getenv("WORKSPACE_ID")
        self.data_table = "ABC_STORE_DATA"
        self.model_table = "ABC_STORE_MODEL"

    def connect(self, **kwargs):
        # config = configparser.ConfigParser()
        # config.read(Path(os.path.realpath(__file__)).parent / "idsw-notebook.conf", encoding="utf-8")
        connection_string = os.getenv("MYSQL_CONNECTION")
        if connection_string is None:
            raise RuntimeError(
                "no proper database connection information provided")
        url = utils.make_url(os.getenv("MYSQL_CONNECTION"))

        try:
            # connection = pymysql.connect(host=config.get("mysql", "host"),
            #                              port=int(config.get("mysql", "port")),
            #                              user=config.get("mysql", "username"),
            #                              password=config.get("mysql", "password"),
            #                              db=config.get("mysql", "db"), **kwargs)
            connection = pymysql.connect(host=url.host,
                                         port=int(url.port),
                                         user=url.username,
                                         password=url.password,
                                         db=url.database, **kwargs)
            self.closed = False
        except Exception as e:
            raise RuntimeError(
                "no proper database connection information provided")

        return connection

    def disconnect(self):
        self.connection.close()
        self.closed = True

    def get_root_path(self):
        return "mysql"

    def get_internal_path(self):
        return "mysql"

    def open_file(self, path):
        """
        open file and read as StringIO on MySQL
        @param path: data path
        @return: StringIO
        """
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
        """
        query blob column and transform to StringIO
        @param table_name:
        @param id:
        @param column:
        @return:
        """
        query_statement = "SELECT " + column + " FROM " + table_name + " WHERE ID= %s"
        blob = None
        from io import StringIO
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query_statement, [id])
                ret = cursor.fetchone()
                blob = ret[0]
                result = StringIO(str(blob, "utf-8"))
        except Exception as e:
            print(e)
            result = None
        return result

    def upload_df(self, df, dataset_name, dataset_desc):
        """
        upload local Pandas.DataFrame as csv file to MySQL and show in Dataset module
        @param df: Pandas.DataFrame
        @param dataset_name: name to show in Dataset module
        @param dataset_desc: description to show in Dataset module
        @return: "success" or None
        """

        if not dataset_name.endswith(".csv"):
            dataset_name += ".csv"
        try:
            # estimate file size
            dataset_size = self._humanbytes(df.memory_usage(index=True).sum())
            # determine file path
            result = self.upload_local_csv(df, dataset_name, dataset_desc, dataset_size)
        except Exception as e:
            print(e)
            result = None
        return result

    def upload_local_csv(self, df, dataset_name="", dataset_desc="", dataset_size=""):
        """
        upload local Pandas.DataFrame as csv file to MySQL and show in Dataset module
        @param df: Pandas.DataFrame
        @param dataset_name: name to show in Dataset module
        @param dataset_desc: description to show in Dataset module
        @param dataset_size: estimated data size
        @return: id or None
        """

        # determine file path
        dst = self.get_root_path() + "/" + self.user_id + \
            "/dataset/" + utils.generate_uuid()
        temp = dst.split("/")
        table = self.data_table
        id = temp[-1]
        # save as csv file to MySQL
        insert_path = self.insert_file(df, table, id)
        if insert_path is not None:
            # insert a record to ABC_DATASET
            resource_id = utils.generate_uuid()
            insert_dataset_statement = """
            INSERT INTO ABC_DATASET (DATASET_ID,WORKSPACE_ID,USER_ID,DATASET_NAME,DATA_DESC,DATASET_TYPE,DATASET_SIZE,DATASET_PATH,IS_ACTIVE,CREATOR,MODIFIER,CREATE_TIME,UPDATE_TIME,START_TIME,END_TIME)
            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            try:
                with self.connection.cursor() as cursor:
                    current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    row_count = cursor.execute(insert_dataset_statement,
                                               [resource_id, self.workspace_id, self.user_id, dataset_name, dataset_desc, "csv",
                                                dataset_size, insert_path, "1", self.user_id, self.user_id, current_time,
                                                current_time, current_time, current_time])
                self.connection.commit()
                if row_count > 0:
                    # insert a record to ABC_RESOURCE_DIR
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
                            return id
                    except Exception as e:
                        print(e)
                        return None
                else:
                    return None
            except Exception as e:
                print(e)
                return None
        else:
            return None

    def insert_file(self, df, table, id):
        insert_statement = "INSERT INTO " + \
            table + " (ID, DATA) values(%s, %s)"
        try:
            with self.connection.cursor() as cursor:
                row_count = cursor.execute(
                    insert_statement, [id, self._convert_to_binary_data(df)])
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
    def _convert_to_binary_data(df):
        from io import BytesIO, StringIO
        bytes_container = StringIO()
        df.to_csv(bytes_container, index=False)
        bytes_container.seek(0)  # update to enable reading
        binary_data = bytes_container.read()
        return binary_data

    @staticmethod
    def _convert_to_binary_model(model):
        from io import BytesIO
        import joblib
        bytes_container = BytesIO()
        joblib.dump(model, bytes_container)
        bytes_container.seek(0)  # update to enable reading
        binary_data = bytes_container.read()
        return binary_data

    def upload_model(self, df, model, model_name, model_desc):
        """
        save sklearn model to inner storage and show in Model module
        @param df: training DataFrame
        @param model: sklearn model
        @param model_name: name to show in Model module
        @param model_desc: model description shown in Model module
        """

        # form the model meta with provided training DataFrame
        meta = self._form_model_meta(df)
        # determine save path
        dst = self.get_root_path() + "/" + self.user_id + \
            "/model/" + utils.generate_uuid()
        temp = dst.split("/")
        table = self.model_table
        id = temp[-1]
        # save the model and meta
        insert_path = self.insert_model(model, meta, table, id)

        if insert_path is None:
            return None

        model_id = utils.generate_uuid()
        resource_dir_id = utils.generate_uuid()
        # insert a record to ABC_MODEL
        insert_model_statement = """
                    INSERT INTO ABC_MODEL (MODEL_ID,WORKSPACE_ID,USER_ID,MODEL_NAME,MODEL_DESC,DEPLOY_ENGINE,MODEL_PATH,MODEL_TYPE,RESOURCE_DIR_ID,IS_ACTIVE,CREATOR,MODIFIER,CREATE_TIME,UPDATE_TIME,START_TIME,END_TIME,MODEL_METADATA)
                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """
        try:
            with self.connection.cursor() as cursor:
                import json
                current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                row_count = cursor.execute(insert_model_statement,
                                           [model_id, self.workspace_id, self.user_id, model_name, model_desc, "", insert_path,
                                            "1", resource_dir_id, "1", self.user_id, self.user_id, current_time,
                                            current_time, current_time, current_time, json.dumps(meta)])
            self.connection.commit()
            if row_count > 0:
                # insert a record to ABC_RESOURCE_DIR
                insert_resource_dir_statement = """
                            INSERT INTO ABC_RESOURCE_DIR (ITEM_ID,WORKSPACE_ID,ITEM_NAME,ITEM_ORDER,PARENT_ID,ITEM_TYPE,IS_LEAF,RESOURCE_ID,IS_SYS,IS_READONLY,IS_DISPLAY,IS_ACTIVE,CREATOR,MODIFIER,CREATE_TIME,UPDATE_TIME,START_TIME,END_TIME,ITEM_DESC,USER_ID)
                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            """
                try:
                    with self.connection.cursor() as cursor:
                        current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        row_count = cursor.execute(insert_resource_dir_statement,
                                                   [utils.generate_uuid(), self.workspace_id, model_name, 1, "-1",
                                                    "0", "1", model_id, "1", "1", "1", "1", self.user_id,
                                                    self.user_id, current_time, current_time, current_time,
                                                    current_time, "", self.user_id])
                        self.connection.commit()
                except Exception as e:
                    print(e)
                    return None
            else:
                return None
        except Exception as e:
            print(e)
            return None

    def insert_model(self, model, meta, table, id):
        insert_statement = "INSERT INTO " + table + \
            " (ID, DATA, META) values(%s, %s, %s)"
        try:
            with self.connection.cursor() as cursor:
                import json
                row_count = cursor.execute(insert_statement,
                                           [id, self._convert_to_binary_model(model), json.dumps(meta)])
                self.connection.commit()
                if row_count > 0:
                    record = table + "/" + id
                else:
                    record = None
        except Exception as e:
            print(e)
            record = None

        return record

    def open_model(self, path):
        """
        load a sklearn model from HDFS path
        @param path: path copied from Model module
        @return: model and meta
        """
        temp = path.split("/")
        table_name = self.model_table
        id = temp[1]
        if len(temp) > 2:
            table_name = temp[-3]
            column = temp[-1]
            id = temp[-2]

        return self.query_model_and_meta(table_name, id)

    def query_model_and_meta(self, table_name, id):
        import joblib
        import json
        from io import BytesIO
        query_statement = "SELECT `DATA`,`META` FROM " + table_name + " WHERE ID= %s"
        model = None
        meta = None
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query_statement, [id])
                ret = cursor.fetchone()
                blob = ret[0]
                model = joblib.load(BytesIO(blob))
                meta = json.loads(ret[1])
        except Exception as e:
            print(e)

        return model, meta
