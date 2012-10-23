# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

install_requires = [
    # d2to1 bootstrap
    'd2to1',

    'pyramid',
    'waitress',
    'onctuous',

    'setuptools >= 0.6b1',
]

tests_requires = [
    'boto',

    'nose',
    'nosexcover',
    'coverage',
    'mock',
    'webtest',
]

setup(
    d2to1=True,
    keywords='pyramid dynamodb mock',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=install_requires,
    tests_require=tests_requires,
    test_suite="tests",
    entry_points = """\
    [paste.app_factory]
    main = ddbmock:main
    """,
)
