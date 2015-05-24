# coding: utf-8

from storage import (
    Storage, KenshinException, InvalidConfig, InvalidTime,
    RetentionParser)

__version__ = "0.1.5"
__commit__ = "63ad1d1"
__author__ = "zhuzhaolong"
__email__ = "zhuzhaolong@douban.com"
__date__ = "Sun May 24 13:32:23 2015 +0800"


_storage = Storage()
validate_archive_list = _storage.validate_archive_list
create = _storage.create
update = _storage.update
fetch = _storage.fetch
header = _storage.header
add_tag = _storage.add_tag

parse_retention_def = RetentionParser.parse_retention_def
