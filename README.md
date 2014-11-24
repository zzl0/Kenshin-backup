
Kenshin
=============

> Kenshin ([るろうに剣心](http://zh.wikipedia.org/wiki/%E6%B5%AA%E5%AE%A2%E5%89%91%E5%BF%83))

![](/img/kenshin.gif)

A scalable time series database.

Installation
-----------------

    $ python setup.py install

Develop
--------------

I recommended using virtualenv when installing dependencies:

    $ virtualenv env
    $ source env/bin/activate
    $ pip install -r requirements.txt

Tests can be run using nosetests:

    $ nosetests -v

TODO
--------------

- web interface
- interface redesign
- name collision
- migration tool