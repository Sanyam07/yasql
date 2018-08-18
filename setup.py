#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'Click>=6.0',
    'sqlparse',
    'funcy',
    'PyYAML',
    'dateparser',
    'lark-parser'
]

setup_requirements = ['pytest-runner', ]

test_requirements = ['pytest', 'freezegun' ]

setup(
    author="Jianye Ye",
    author_email='yejianye@gmail.com',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    description="Write SQL in YAML-format",
    entry_points={
        'console_scripts': [
            'yasql=yasql.cli:main',
        ],
    },
    install_requires=requirements,
    license="MIT license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='yasql',
    name='yasql',
    packages=find_packages(include=['yasql']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/yejianye/yasql',
    version='0.1.0',
    zip_safe=False,
)
