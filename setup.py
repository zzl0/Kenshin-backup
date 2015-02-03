# coding: utf-8

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

config = {
    'description': 'A scalable time series database.',
    'author': 'Zhaolong Zhu',
    'url': 'http://code.dapps.douban.com/Kenshin',
    'download_url': 'http://code.dapps.douban.com/Kenshin.git',
    'author_email': 'zhuzhaolong0@gmail.com',
    'version': '0.1',
    'install_requires': [],
    'test_require': ['nose'],
    'packages': ['kenshin', 'kenshin.tools', 'rurouni'],
    'scripts': [],
    'name': 'kenshin'
}

setup(**config)
