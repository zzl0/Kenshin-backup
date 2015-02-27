# coding: utf-8

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

from glob import glob


setup(
    name='kenshin',
    version='0.1.0',
    description='A scalable time series database.',
    author='Zhaolong Zhu',
    url='http://code.dapps.douban.com/Kenshin',
    download_url='http://code.dapps.douban.com/Kenshin.git',
    author_email='zhuzhaolong0@gmail.com',
    install_requires=[],
    test_require=['nose'],
    packages=['kenshin', 'kenshin.tools', 'rurouni', 'rurouni.state', 'twisted.plugins'],
    scripts=glob('bin/*'),
)
