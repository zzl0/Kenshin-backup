# coding: utf-8

from kenshin.storage import (
    Storage, KenshinException, InvalidConfig, InvalidTime,
    RetentionParser)

__version__ = "0.2.1"
__commit__ = "03dda36"
__author__ = "zhuzhaolong"
__email__ = "zhuzhaolong0@gmail.com"
__date__ = "Thu Sep 24 11:09:50 2015 +0800"


_storage = Storage()
validate_archive_list = _storage.validate_archive_list
create = _storage.create
update = _storage.update
fetch = _storage.fetch
header = _storage.header
pack_header = _storage.pack_header
add_tag = _storage.add_tag

parse_retention_def = RetentionParser.parse_retention_def
