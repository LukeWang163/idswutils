#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019-12-10 16:55
# @Author  : Luke
# @File    : iminio.py
# @Desc    : minio connection
import os
from minio import Minio
from minio.error import (ResponseError, BucketAlreadyOwnedByYou,
                         BucketAlreadyExists)
from . import utils
from . import connection
from pathlib2 import Path
import datetime


class MinIOConnection(connection.Connection):
    def __init__(self):
        super().__init__()
        self.connection = self.connect()
        self.user_id = os.getenv("USER_ID")
        self.workspace_id = os.getenv("WORKSPACE_ID")
        self.bucket = os.getenv("MINIO_BUCKET", "idsw")
        try:
            if not self.connection.bucket_exists(self.bucket):
                self.connection.make_bucket(self.bucket)
        except ResponseError as err:
            print(err)

    def connect(self, host=None, port=None, hdfs_ha_enable=None, user=None, kerberos=False):
        endpoint = os.getenv("MINIO_ENDPOINT")
        access_key = os.getenv("MINIO_ACCESSKEY", "admin")
        secret_key = os.getenv("MINIO_SECRETKEY", "admin123")

        # Initialize client with an endpoint and access/secret keys.
        client = Minio(endpoint,
                            access_key=access_key,
                            secret_key=secret_key,
                            secure=False)
        self.closed = False
        return client

    def disconnect(self):
        self.connection = None
        self.closed = True

    def get_root_path(self):
        return "idsw"

    def get_internal_path(self):
        return "idsw"

    def open_file(self, path):
        """
        open file and read as StringIO on MinIO
        @param path: data path
        @return: StringIO
        """
        from io import StringIO
        with self.connection.get_object(self.bucket, path) as f:
            data = StringIO(str(f.read(), "utf-8"))
        return data

    def upload_df(self, df, dataset_name, dataset_desc):
        """
        upload local Pandas.DataFrame as csv file to minio and show in Dataset module
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
            dst = self.get_root_path() + "/" + self.user_id + "/dataset/" + utils.generate_uuid()
            from io import BytesIO
            csv_bytes = df.to_csv(index=False).encode('utf-8')
            csv_buffer = BytesIO(csv_bytes)

            self.connection.put_object(self.bucket,
                                   dst,
                                   data=csv_buffer,
                                   length=len(csv_bytes),
                                   content_type='application/csv')

            # insert a record to ABC_DATASET
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
                                               [dataset_id, self.workspace_id, self.user_id, dataset_name, dataset_desc,
                                                "csv",
                                                dataset_size, dst, resource_dir_id, "1", self.user_id, self.user_id,
                                                current_time,
                                                current_time, current_time, current_time])
                mysql_connection.commit()
                if row_count > 0:
                    # insert a record to ABC_RESOURCE_DIR
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
        """
        load a sklearn model from minio path
        @param path: path copied from Model module
        @return: model and meta
        """

        # specify model and meta location
        model_path = str(Path(path) / "data")
        meta_path = str(Path(path) / "meta")
        import joblib
        from io import BytesIO
        model = None
        meta = None
        # load the model
        with self.connection.get_object(self.bucket, model_path) as model_reader:
            try:
                model = joblib.load(BytesIO(model_reader.read()))
            except Exception as e:
                print(e)
        # load the meta
        with self.connection.get_object(self.bucket, meta_path) as json_reader:
            try:
                import json
                meta = json.load(BytesIO(json_reader.read()))
            except Exception as e:
                print(e)

        return model, meta

    def upload_model(self, df, model, model_name, model_desc):
        """
        save sklearn model to inner storage and show in Model module
        @param df: training DataFrame
        @param model: sklearn model
        @param model_name: name to show in Model module
        @param model_desc: description to show in Model module
        """

        # form the model meta with provided training DataFrame
        meta = self._form_model_meta(df)
        # determine save path
        dst = self.get_root_path() + "/" + self.user_id + "/model/" + utils.generate_uuid()
        import joblib
        import json
        # save the model
        from io import BytesIO
        model_bytes_container = BytesIO()
        joblib.dump(model, model_bytes_container)
        model_bytes = model_bytes_container.tell()
        model_bytes_container.seek(0)  # update to enable reading

        self.connection.put_object(self.bucket,
                                   dst + "/" + "data",
                                   data=model_bytes_container,
                                   length=model_bytes)

        meta_bytes_container = BytesIO()
        meta_bytes_container.write(json.dumps(meta).encode("utf-8"))
        meta_bytes = meta_bytes_container.tell()
        meta_bytes_container.seek(0)  # update to enable reading
        # save the meta
        self.connection.put_object(self.bucket,
                                   dst + "/" + "meta",
                                   data=meta_bytes_container,
                                   length=meta_bytes)

        # insert a record to ABC_MODEL
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
                                           [model_id, self.workspace_id, self.user_id, model_name, model_desc, "",
                                            dst, "1", resource_dir_id, "1", self.user_id, self.user_id,
                                            current_time, current_time, current_time, current_time, json.dumps(meta)])
            mysql_connection.commit()
            if row_count > 0:
                # insert a record to ABC_RESOURCE_DIR
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
                        return "success"
                except Exception as e:
                    print(e)
                    return None
            else:
                return None
        except Exception as e:
            print(e)
            return None
