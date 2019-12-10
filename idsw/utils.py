#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2019-12-10 15:33
# @Author  : Luke
# @File    : utils.py.py
# @Desc    :

import uuid


def generate_uuid():
    return str(uuid.uuid1()).replace("-", "")
