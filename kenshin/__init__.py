# coding: utf-8

from kenshin.storage import (
    Storage, KenshinException, InvalidConfig, InvalidTime,
    RetentionParser)

__version__ = "0.1.5"
__commit__ = "22a334c"
__author__ = "zhuzhaolong"
__email__ = "zhuzhaolong0@gmail.com"
__date__ = "Tue Sep 1 15:18:55 2015 +0800"


_storage = Storage()
validate_archive_list = _storage.validate_archive_list
create = _storage.create
update = _storage.update
fetch = _storage.fetch
header = _storage.header
pack_header = _storage.pack_header
add_tag = _storage.add_tag

parse_retention_def = RetentionParser.parse_retention_def
