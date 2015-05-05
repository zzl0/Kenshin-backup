# coding: utf-8

from storage import (
    Storage, KenshinException, InvalidConfig, InvalidTime,
    RetentionParser)

__version__ = "0.1.2"
__commit__ = "7855e60"
__author__ = "zhuzhaolong"
__email__ = "zhuzhaolong0@gmail.com"
__date__ = "Tue May 5 11:36:54 2015 +0800"


_storage = Storage()
validate_archive_list = _storage.validate_archive_list
create = _storage.create
update = _storage.update
fetch = _storage.fetch
header = _storage.header
add_tag = _storage.add_tag

parse_retention_def = RetentionParser.parse_retention_def
