#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019-12-10 16:55
# @Author  : Luke
# @File    : connection.py
# @Desc    : connection class


class Connection:
    def __init__(self):
        self.connection = None
        self.closed = True

    def connect(self, **kwargs):
        pass

    def disconnect(self):
        pass
    
    def get_root_path(self):
        pass

    def get_internal_path(self):
        pass

    def open_file(self, path):
        pass

    def upload_df(self, df, dataset_name):
        pass

    def open_model(self, path):
        pass

    def upload_model(self, df, model, model_name):
        pass

    def _humanbytes(self, B):
        # Return the given bytes as a human friendly KB, MB, GB, or TB string
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

    def _form_model_meta(self, df):
        from collections import OrderedDict
        return df.dtypes.apply(lambda x: x.name).to_dict(into=OrderedDict)
