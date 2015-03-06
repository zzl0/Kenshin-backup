# coding: utf-8

from storage import (
    Storage, KenshinException, InvalidConfig, InvalidTime,
    RetentionParser)

__version__ = "0.1.0"
__commit__ = "984b8f7"
__author__ = "zhuzhaolong"
__email__ = "zhuzhaolong0@gmail.com"
__date__ = "Fri Mar 6 16:05:54 2015 +0800"


_storage = Storage()
validate_archive_list = _storage.validate_archive_list
create = _storage.create
update = _storage.update
fetch = _storage.fetch
header = _storage.header
add_tag = _storage.add_tag

parse_retention_def = RetentionParser.parse_retention_def