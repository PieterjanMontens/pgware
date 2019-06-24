#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import find_packages, setup

try:
    from setuphelpers import find_version, long_description
except ImportError:
    # 999 to solve CI install problem
    find_version = lambda x: "999.0.0" # noqa
    long_description = lambda: __doc__ # noqa

setup(
    name="pgware",
    version=find_version("pgware/__init__.py"),
    description=('PostgreSQL Advanced Adapter Wrapper'),
    long_description=long_description(),
    packages=find_packages(),
    install_requires=["py-dateutil", "asyncpg", "psycopg2-binary"],
    extras_require={},
    python_requires=">=3.7",
    setup_requires=["setuphelpers"],
    tests_require=["pytest", "pytest-asyncio", "pytest-cov", "coverage", "pylint", "flake8"],
    test_suite="tests"
)
